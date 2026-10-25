"""MarketIntelSource backed by the Unusual Whales REST API.

Fills the slot ginlix-data holds on the hosted platform, in the same shapes:
contract tickers in the ``O:`` form, short-volume ratio as a percent. UW
filters a chain only by single expiry, type and days to expiry, so date and
strike ranges are applied here.
"""

from __future__ import annotations

import asyncio
import re
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

_BASE_URL = "https://api.unusualwhales.com"
_ET = ZoneInfo("America/New_York")
_PAGE = 500  # UW's max ``limit`` on option-contracts
_MAX_PAGES = 20
_OSI = re.compile(r"^(.+?)(\d{6})([CP])(\d{8})$")
# Intraday contract bars are built from UW's 1-minute bars, one request per
# trading day, so a window is capped to its most recent days.
_SPAN_S = {"1min": 60, "5min": 300, "15min": 900, "30min": 1800, "1hour": 3600, "4hour": 14400}
_MAX_INTRADAY_DAYS = 30


def parse_osi(symbol: str) -> tuple[str, str, str, float]:
    """``AAPL261120P00300000`` → (underlying, expiry, contract type, strike)."""
    m = _OSI.match(symbol.removeprefix("O:"))
    if m is None:
        raise ValueError(f"Not an OSI option symbol: {symbol}")
    root, yymmdd, cp, strike = m.groups()
    expiry = f"20{yymmdd[:2]}-{yymmdd[2:4]}-{yymmdd[4:]}"
    return root, expiry, "call" if cp == "C" else "put", int(strike) / 1000


def _num(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def contract_row(row: dict[str, Any]) -> dict[str, Any]:
    sym = row["option_symbol"]
    underlying, expiry, ctype, strike = parse_osi(sym)
    return {
        "ticker": "O:" + sym,
        "underlying_ticker": underlying,
        "contract_type": ctype,
        "strike_price": strike,
        "expiration_date": expiry,
        "shares_per_contract": 100,
        "open_interest": row.get("open_interest"),
        "implied_volatility": _num(row.get("implied_volatility")),
        "delta": _num(row.get("delta")),
        "gamma": _num(row.get("gamma")),
        "theta": _num(row.get("theta")),
        "vega": _num(row.get("vega")),
    }


def _in_range(c: dict[str, Any], f: dict[str, Any]) -> bool:
    exp, strike = c["expiration_date"], c["strike_price"]
    return (
        (not f.get("expiration_date_gte") or exp >= f["expiration_date_gte"])
        and (not f.get("expiration_date_lte") or exp <= f["expiration_date_lte"])
        and (
            f.get("strike_price_gte") is None or strike >= float(f["strike_price_gte"])
        )
        and (
            f.get("strike_price_lte") is None or strike <= float(f["strike_price_lte"])
        )
    )


def _us_market_status() -> str:
    # ponytail: ET clock and weekdays, not exchange_calendars, which the sandbox
    # image lacks; an exchange holiday reads as a normal day.
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return "closed"
    hm = now.strftime("%H:%M")
    if "04:00" <= hm < "09:30":
        return "early_trading"
    if "09:30" <= hm < "16:00":
        return "open"
    return "late_trading" if "16:00" <= hm < "20:00" else "closed"


def _merge(bars: list[dict[str, Any]], start: Any) -> list[dict[str, Any]]:
    """Ascending bars folded into one bar per period, stamped at the period's
    start (epoch ms) that ``start`` gives for each bar."""
    out: list[dict[str, Any]] = []
    last: Any = None
    for b in bars:
        k = start(b)
        if k != last:
            out.append({**b, "time": k})
            last = k
            continue
        cur = out[-1]
        cur["high"] = max((x for x in (cur["high"], b["high"]) if x is not None), default=None)
        cur["low"] = min((x for x in (cur["low"], b["low"]) if x is not None), default=None)
        cur["close"] = b["close"]
        cur["volume"] += b["volume"]
    return out


def _et_day(bar: dict[str, Any]) -> date:
    return datetime.fromtimestamp(bar["time"] / 1000, _ET).date()


def _et_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=_ET).timestamp() * 1000)


def _bucket(span_ms: int) -> Any:
    """Intraday buckets counted from ET midnight, so a 4-hour bar keeps its ET
    boundaries across a daylight-saving change."""
    def start(bar: dict[str, Any]) -> int:
        midnight = _et_ms(_et_day(bar))
        return midnight + (bar["time"] - midnight) // span_ms * span_ms
    return start


def _sorted(
    rows: list[dict[str, Any]], sort: str, key: str, limit: int
) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda r: r[key], reverse=sort.endswith(".desc"))[:limit]


class UnusualWhalesIntelSource:
    """Options chains, contract quotes and short data from Unusual Whales."""

    def __init__(self, api_key: str) -> None:
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            },
            timeout=20.0,
        )

    async def _get(self, path: str, params: Any = None) -> dict[str, Any]:
        resp = await self._client.get(path, params=params)
        if resp.status_code != 200:
            try:
                body = resp.json()
                detail = f"{body.get('code')}: {body.get('message')}"
            except ValueError:
                detail = resp.text[:200]
            raise RuntimeError(f"Unusual Whales {path} failed: {detail} ({resp.status_code})")
        return resp.json()

    async def get_options_chain(
        self, underlying: str, user_id: str | None = None, **filters: Any
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": _PAGE}
        if filters.get("contract_type"):
            params["option_type"] = filters["contract_type"]
        gte, lte = (
            filters.get("expiration_date_gte"),
            filters.get("expiration_date_lte"),
        )
        if gte and gte == lte:
            params["expiry"] = gte
        else:
            # Narrow server-side by days to expiry, a day wider each side
            # since UW counts from its own today; _in_range is exact.
            today = datetime.now(_ET).date()
            if gte:
                params["min_dte"] = max((date.fromisoformat(gte) - today).days - 1, 0)
            if lte:
                params["max_dte"] = (date.fromisoformat(lte) - today).days + 1
        # The cursor is UW's page number. A page whose rows all fall outside
        # the strike range is skipped rather than returned empty, since an
        # empty page ends the caller's cursor walk.
        page = int(filters.get("cursor") or 0)
        for _ in range(_MAX_PAGES):
            raw = (
                await self._get(
                    f"/api/stock/{underlying.upper()}/option-contracts",
                    {**params, "page": page},
                )
            ).get("data") or []
            rows = [c for c in map(contract_row, raw) if _in_range(c, filters)]
            page += 1
            more = len(raw) == _PAGE
            if rows or not more:
                break
        rows.sort(
            key=lambda c: (c["expiration_date"], c["strike_price"], c["contract_type"])
        )
        return {"results": rows, "next_cursor": str(page) if more else None}

    async def get_options_snapshot(
        self, tickers: list[str], user_id: str | None = None
    ) -> list[dict[str, Any]]:
        by_underlying: dict[str, list[str]] = {}
        for t in tickers:
            by_underlying.setdefault(parse_osi(t)[0], []).append(t.removeprefix("O:"))
        status = _us_market_status()
        out = []
        for underlying, symbols in by_underlying.items():
            for i in range(0, len(symbols), _PAGE):
                chunk = symbols[i : i + _PAGE]
                params = [("option_symbol[]", s) for s in chunk] + [("limit", _PAGE)]
                for row in (
                    await self._get(f"/api/stock/{underlying}/option-contracts", params)
                ).get("data") or []:
                    bid, ask, last = (
                        _num(row.get("nbbo_bid")),
                        _num(row.get("nbbo_ask")),
                        _num(row.get("last_price")),
                    )
                    out.append(
                        {
                            "ticker": "O:" + row["option_symbol"],
                            "market_status": status,
                            # ponytail: UW's contract row has no open or prior close, so no
                            # open or change_percent; /option-contract/{id}/historic has them.
                            "session": {
                                "open": None,
                                "high": _num(row.get("high_price")),
                                "low": _num(row.get("low_price")),
                                "close": last,
                                "change_percent": None,
                                "volume": row.get("volume"),
                            },
                            "last_quote": {
                                "bid": bid,
                                "ask": ask,
                                "midpoint": round((bid + ask) / 2, 4)
                                if bid is not None and ask is not None
                                else None,
                            },
                            "last_trade": {"price": last},
                            "open_interest": row.get("open_interest"),
                            "implied_volatility": _num(row.get("implied_volatility")),
                        }
                    )
        return out

    async def get_options_ohlcv(
        self,
        options_ticker: str,
        from_date: str | None = None,
        to_date: str | None = None,
        interval: str = "1hour",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        sym = options_ticker.removeprefix("O:")
        if interval in _SPAN_S:
            minutes = await self._contract_minutes(sym, from_date, to_date)
            # Stamped at the bucket's start, as ginlix-data's aggregates are.
            return _merge(minutes, _bucket(_SPAN_S[interval] * 1000))
        # The window stays the caller's, as ginlix-data's does; only the stamp
        # moves to the period's first day.
        daily = await self._contract_days(sym, from_date, to_date)
        if interval == "1day":
            return daily
        if interval == "1week":
            return _merge(daily, lambda b: _et_ms(_et_day(b) - timedelta(_et_day(b).weekday())))
        if interval == "1month":
            return _merge(daily, lambda b: _et_ms(_et_day(b).replace(day=1)))
        raise ValueError(f"Interval '{interval}' is not supported by Unusual Whales")

    async def _contract_days(
        self, sym: str, from_date: str | None, to_date: str | None
    ) -> list[dict[str, Any]]:
        rows = (await self._get(f"/api/option-contract/{sym}/historic")).get("chains") or []
        bars = []
        for r in rows:
            d = r.get("date")
            if not d or (from_date and d < from_date) or (to_date and d > to_date):
                continue
            bars.append({
                "time": int(datetime.fromisoformat(d).replace(tzinfo=_ET).timestamp() * 1000),
                "open": _num(r.get("open_price")),
                "high": _num(r.get("high_price")),
                "low": _num(r.get("low_price")),
                "close": _num(r.get("last_price")),
                "volume": r.get("volume") or 0,
            })
        return sorted(bars, key=lambda b: b["time"])

    async def _contract_minutes(
        self, sym: str, from_date: str | None, to_date: str | None
    ) -> list[dict[str, Any]]:
        if to_date:
            end = date.fromisoformat(to_date)
        else:
            now = datetime.now(_ET)
            end = now.date() if now.strftime("%H:%M") >= "09:30" else now.date() - timedelta(1)
            while end.weekday() >= 5:
                end -= timedelta(1)
        start = date.fromisoformat(from_date) if from_date else end
        # A holiday among the weekdays costs one empty intraday call.
        days = [
            d for d in (start + timedelta(n) for n in range((end - start).days + 1))
            if d.weekday() < 5
        ][-_MAX_INTRADAY_DAYS:]
        pages = await asyncio.gather(*(
            self._get(f"/api/option-contract/{sym}/intraday", {"date": d.isoformat()}) for d in days
        ))
        bars = []
        for page in pages:
            for r in page.get("data") or []:
                stamp = r.get("start_time")
                if not stamp:
                    continue
                bars.append({
                    "time": int(datetime.fromisoformat(stamp).timestamp() * 1000),
                    "open": _num(r.get("open")),
                    "high": _num(r.get("high")),
                    "low": _num(r.get("low")),
                    "close": _num(r.get("close")),
                    # UW splits a minute's volume by side; the four sides sum to it.
                    "volume": sum(r.get(k) or 0 for k in (
                        "volume_ask_side", "volume_bid_side", "volume_mid_side", "volume_no_side",
                    )),
                })
        return sorted(bars, key=lambda b: b["time"])

    async def _interest_float(self, symbol: str) -> list[dict[str, Any]]:
        return (await self._get(f"/api/shorts/{symbol.upper()}/interest-float/v2")).get(
            "data"
        ) or []

    async def get_short_interest(
        self,
        symbol: str,
        limit: int = 500,
        sort: str = "settlement_date.asc",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            {
                "ticker": r.get("symbol") or symbol,
                "settlement_date": r["market_date"],
                "short_interest": r.get("short_interest"),
                "days_to_cover": _num(r.get("days_to_cover")),
            }
            for r in await self._interest_float(symbol)
            if r.get("market_date")
        ]
        return _sorted(rows, sort, "settlement_date", limit)

    async def get_short_volume(
        self,
        symbol: str,
        limit: int = 500,
        sort: str = "date.asc",
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        body = await self._get(f"/api/shorts/{symbol.upper()}/volume-and-ratio")
        rows = []
        for r in body.get("si") or []:  # UW returns these rows under ``si``
            ratio = _num(r.get("short_volume_ratio"))
            if r.get("market_date"):
                rows.append(
                    {
                        "ticker": symbol,
                        "date": r["market_date"],
                        "short_volume": r.get("short_volume"),
                        "total_volume": r.get("total_volume"),
                        "short_volume_ratio": round(ratio * 100, 2)
                        if ratio is not None
                        else None,
                    }
                )
        return _sorted(rows, sort, "date", limit)

    async def get_float_shares(
        self, symbol: str, user_id: str | None = None
    ) -> dict[str, Any]:
        rows = [
            r
            for r in await self._interest_float(symbol)
            if r.get("total_float") and r.get("market_date")
        ]
        if not rows:
            return {}
        latest = max(rows, key=lambda r: r["market_date"])
        return {
            "ticker": symbol,
            "free_float": latest["total_float"],
            "effective_date": latest["market_date"],
        }

    async def get_movers(
        self, direction: str = "gainers", user_id: str | None = None
    ) -> list[dict[str, Any]]:
        # ponytail: /api/market/movers needs UW's Advanced tier, and its row
        # shape is unverified until a key with that tier can read it.
        raise RuntimeError(
            "Market movers are not available from Unusual Whales on this API tier"
        )

    async def close(self) -> None:
        await self._client.aclose()
