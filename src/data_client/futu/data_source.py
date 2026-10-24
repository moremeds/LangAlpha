"""MarketDataSource backed by the user's own Futu OpenD gateway.

OpenD is a local process the user runs and logs in to; this source only reads
quotes and K-lines through it, with whatever quote rights that account holds.
The SDK is synchronous and keeps one socket per context, so calls run in a
worker thread, one at a time per process.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import socket
import threading
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from market_protocol import to_canonical
from market_protocol.enums import AssetClass
from market_protocol.symbology import market_of

from src.utils.market_hours import interval_seconds

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")

# data_client interval → Futu KLType name. 4hour has no Futu K-line, so it
# raises ValueError and the chain moves on.
_KTYPE = {
    "1min": "K_1M",
    "5min": "K_5M",
    "15min": "K_15M",
    "30min": "K_30M",
    "1hour": "K_60M",
}

_DEFAULT_LOOKBACK = {
    "K_1M": timedelta(days=6),
    "K_5M": timedelta(days=57),
    "K_15M": timedelta(days=57),
    "K_30M": timedelta(days=57),
    "K_60M": timedelta(days=727),
    "K_DAY": timedelta(days=730),
}

# Index families market_protocol places on the HK calendar. OpenD refuses US
# indices outright ("US stock indices are not supported").
_HK_INDEX = {"HSI": "HK.800000", "HSCE": "HK.800100"}

_CN_MIC = {"XSHG": "SH", "XSHE": "SZ"}

_CALL_TIMEOUT_S = 15.0
_SNAPSHOT_BATCH = 400  # OpenD's per-request cap for get_market_snapshot
_MAX_PAGES = 50


def _opend_address() -> tuple[str, int]:
    return os.getenv("FUTU_OPEND_HOST", "127.0.0.1"), int(
        os.getenv("FUTU_OPEND_PORT", "11111")
    )


def futu_code(symbol: str, is_index: bool = False) -> tuple[str, ZoneInfo]:
    """The OpenD code for a chain symbol, with its exchange's timezone.

    Raises ValueError for anything OpenD cannot serve, so the chain skips it.
    """
    ref = to_canonical(symbol, asset_class=AssetClass.INDEX if is_index else None)
    tz = ZoneInfo(ref.tz)
    market = market_of(ref)
    if ref.asset_class == AssetClass.INDEX:
        code = _HK_INDEX.get(ref.index_family or "") if market == "hk" else None
        if code is None:
            raise ValueError(f"Futu OpenD does not serve index {symbol}")
        return code, tz
    if market == "us":
        return "US." + ref.symbol.replace("-", "."), tz
    if market == "hk":
        return "HK." + ref.symbol.zfill(5), tz
    if market == "cn" and ref.mic in _CN_MIC:
        return f"{_CN_MIC[ref.mic]}.{ref.symbol}", tz
    raise ValueError(f"Futu OpenD does not serve {symbol}")


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _local_ms(stamp: str, tz: ZoneInfo) -> int:
    """OpenD stamps are exchange-local wall time, optionally with milliseconds."""
    return int(datetime.fromisoformat(stamp).replace(tzinfo=tz).timestamp() * 1000)


def bars_from_klines(
    rows: list[dict[str, Any]], tz: ZoneInfo, interval: str | None
) -> list[dict[str, Any]]:
    """K-line rows to chain bars, each stamped at its start.

    OpenD labels a minute bar by its close (the first regular 1-minute bar is
    09:31), so a bar starts one span earlier, or at the previous bar's close
    when that is later: the half-hour bar that ends the US session at 16:00,
    and the first bar after a lunch break, are shorter than the span.
    """
    span_ms = interval_seconds(interval) * 1000 if interval else 0
    bars: list[dict[str, Any]] = []
    prev_end = 0
    for row in rows:
        o, h, lo, c = (_finite(row.get(k)) for k in ("open", "high", "low", "close"))
        if None in (o, h, lo, c):
            continue
        end = _local_ms(row["time_key"], tz)
        start = max(end - span_ms, prev_end) if span_ms else end
        prev_end = end
        vol = _finite(row.get("volume"))
        bars.append(
            {
                "time": start,
                "open": o,
                "high": h,
                "low": lo,
                "close": c,
                "volume": int(vol) if vol is not None else 0,
            }
        )
    return bars


def _us_status(phase: str) -> str:
    return {"pre": "early_trading", "open": "open", "post": "late_trading"}.get(
        phase, "closed"
    )


def snapshot_row(
    row: dict[str, Any], symbol: str, tz: ZoneInfo, us_phase: str | None
) -> dict[str, Any] | None:
    price = _finite(row.get("last_price"))
    if not price:
        return None
    prev = _finite(row.get("prev_close_price"))
    change = price - prev if prev else None
    vol = _finite(row.get("volume"))
    stamp = row.get("update_time")
    return {
        "symbol": symbol,
        "name": row.get("name") or None,
        "price": price,
        "change": round(change, 4) if change is not None else None,
        "change_percent": round(change / prev * 100, 4) if change is not None else None,
        "previous_close": prev,
        "open": _finite(row.get("open_price")),
        "high": _finite(row.get("high_price")),
        "low": _finite(row.get("low_price")),
        "volume": int(vol) if vol is not None else None,
        "market_status": _us_status(us_phase) if us_phase else None,
        "early_trading_change_percent": None,
        "late_trading_change_percent": None,
        "as_of": _local_ms(stamp, tz) if stamp and stamp != "N/A" else None,
        # ponytail: regular-session price only; OpenD also has pre_price/after_price
        # if extended-hours quotes are wanted.
        "regular_only": True,
    }


class FutuDataSource:
    """Quotes and K-lines from a Futu OpenD gateway."""

    def __init__(self) -> None:
        self._ctx: Any = None
        # ponytail: one OpenD socket per process, calls serialized; a context
        # pool would be the upgrade if quote latency under load matters.
        self._lock = threading.Lock()

    def _context(self) -> Any:
        if self._ctx is None:
            import futu
            from futu.common.ft_logger import logger as futu_logger

            host, port = _opend_address()
            # The SDK constructor retries a refused connection forever, so a
            # stopped OpenD would hang the chain instead of failing over.
            socket.create_connection((host, port), timeout=2).close()
            futu.SysConfig.set_all_thread_daemon(True)
            futu_logger.console_level = logging.WARNING
            self._ctx = futu.OpenQuoteContext(host=host, port=port)
        return self._ctx

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """One OpenD request, returning its payload or raising on RET_ERROR."""
        from futu import RET_OK

        with self._lock:
            ret, data, *rest = getattr(self._context(), method)(*args, **kwargs)
        if ret != RET_OK:
            raise RuntimeError(f"Futu OpenD {method}: {data}")
        return (data, *rest) if rest else data

    async def _run(self, fn: Any, *args: Any) -> Any:
        return await asyncio.wait_for(asyncio.to_thread(fn, *args), _CALL_TIMEOUT_S)

    def _klines(
        self, code: str, ktype: str, start: str | None, end: str | None
    ) -> list[dict[str, Any]]:
        from futu import AuType

        if not start:
            start = (datetime.now(_ET) - _DEFAULT_LOOKBACK[ktype]).strftime("%Y-%m-%d")
        rows: list[dict[str, Any]] = []
        page_key = None
        for _ in range(_MAX_PAGES):
            # QFQ: forward-adjusted for splits and dividends, declared
            # dividend_adjusted in normalize._DECLARED.
            df, page_key = self._call(
                "request_history_kline",
                code,
                start=start,
                end=end,
                ktype=ktype,
                autype=AuType.QFQ,
                max_count=1000,
                page_req_key=page_key,
            )
            rows.extend(df.to_dict("records"))
            if page_key is None:
                break
        return rows

    async def get_intraday(
        self,
        symbol: str,
        interval: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        ktype = _KTYPE.get(interval)
        if ktype is None:
            raise ValueError(f"Interval '{interval}' is not supported by Futu OpenD")
        code, tz = futu_code(symbol, is_index)
        rows = await self._run(self._klines, code, ktype, from_date, to_date)
        return bars_from_klines(rows, tz, interval)

    async def get_daily(
        self,
        symbol: str,
        from_date: str | None = None,
        to_date: str | None = None,
        is_index: bool = False,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        code, tz = futu_code(symbol, is_index)
        rows = await self._run(self._klines, code, "K_DAY", from_date, to_date)
        return bars_from_klines(rows, tz, None)

    def _snapshots(self, codes: list[str]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for i in range(0, len(codes), _SNAPSHOT_BATCH):
            batch = codes[i : i + _SNAPSHOT_BATCH]
            try:
                df = self._call("get_market_snapshot", batch)
            except RuntimeError:
                if len(batch) == 1:
                    raise
                # OpenD fails the whole batch for one code the account has no
                # rights to, so ask one at a time to keep the rest.
                for code in batch:
                    try:
                        out.update(self._snapshots([code]))
                    except RuntimeError as exc:
                        logger.info("futu.snapshot.refused | code=%s | %s", code, exc)
                continue
            out.update({r["code"]: r for r in df.to_dict("records")})
        return out

    async def get_snapshots(
        self,
        symbols: list[str],
        asset_type: str = "stocks",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        from src.utils.market_hours import current_market_phase

        wanted: dict[str, tuple[str, ZoneInfo]] = {}
        for s in symbols:
            try:
                wanted[s] = futu_code(s, is_index=(asset_type == "indices"))
            except ValueError:
                continue  # a symbol OpenD cannot serve is a miss, not a failure
        if not wanted:
            return []
        rows = await self._run(self._snapshots, sorted({c for c, _ in wanted.values()}))
        us_phase = current_market_phase()
        out = []
        for symbol, (code, tz) in wanted.items():
            row = rows.get(code)
            snap = row and snapshot_row(
                row, symbol, tz, us_phase if code.startswith("US.") else None
            )
            if snap:
                out.append(snap)
        return out

    async def get_market_status(self, user_id: str | None = None) -> dict[str, Any]:
        from src.utils.market_hours import current_market_phase

        phase = current_market_phase()
        return {
            "market": "open"
            if phase == "open"
            else ("extended-hours" if phase in ("pre", "post") else "closed"),
            "afterHours": phase == "post",
            "earlyHours": phase == "pre",
            "serverTime": datetime.now(_ET).isoformat(),
            "exchanges": None,
        }

    async def close(self) -> None:
        if self._ctx is not None:
            await asyncio.to_thread(self._ctx.close)
            self._ctx = None
