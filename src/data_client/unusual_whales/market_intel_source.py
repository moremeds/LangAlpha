"""MarketIntelSource backed by the Unusual Whales REST API.

Fills the slot ginlix-data holds on the hosted platform, in the same shapes:
contract tickers in the ``O:`` form, short-volume ratio as a percent. UW
filters a chain only by single expiry, type and days to expiry, so date and
strike ranges are applied here.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

_BASE_URL = "https://api.unusualwhales.com"
_ET = ZoneInfo("America/New_York")
_PAGE = 500  # UW's max ``limit`` on option-contracts
_MAX_PAGES = 20
_OSI = re.compile(r"^(.+?)(\d{6})([CP])(\d{8})$")


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
            raise RuntimeError(
                f"Unusual Whales {path} returned {resp.status_code} ({detail})"
            )
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
        from src.utils.market_hours import current_market_phase

        by_underlying: dict[str, list[str]] = {}
        for t in tickers:
            by_underlying.setdefault(parse_osi(t)[0], []).append(t.removeprefix("O:"))
        status = {"pre": "early_trading", "open": "open", "post": "late_trading"}.get(
            current_market_phase(), "closed"
        )
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
                            # ponytail: UW's contract row has no prior close, so no
                            # change_percent; /option-contract/{id}/historic has it per contract.
                            "session": {
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
        if interval != "1day":
            # ponytail: daily only; UW's /option-contract/{id}/intraday has
            # 1-minute bars one day per call, add when a caller needs them.
            raise ValueError(
                f"Interval '{interval}' is not supported by Unusual Whales"
            )
        sym = options_ticker.removeprefix("O:")
        rows = (await self._get(f"/api/option-contract/{sym}/historic")).get(
            "chains"
        ) or []
        bars = []
        for r in rows:
            d = r.get("date")
            if not d or (from_date and d < from_date) or (to_date and d > to_date):
                continue
            bars.append(
                {
                    "time": int(
                        datetime.fromisoformat(d).replace(tzinfo=_ET).timestamp() * 1000
                    ),
                    "open": _num(r.get("open_price")),
                    "high": _num(r.get("high_price")),
                    "low": _num(r.get("low_price")),
                    "close": _num(r.get("last_price")),
                    "volume": r.get("volume") or 0,
                }
            )
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
