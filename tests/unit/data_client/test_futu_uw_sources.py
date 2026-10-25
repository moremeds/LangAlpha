"""Futu K-line timing and Unusual Whales chain parsing.

Fixtures are real rows read once on 2026-10-24 for the 2026-10-23 session:
Futu OpenD hourly K-lines (qfq) and Unusual Whales option-contracts rows.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from src.data_client.futu.data_source import bars_from_klines, futu_code
from src.data_client.unusual_whales.market_intel_source import (
    UnusualWhalesIntelSource,
    _in_range,
    contract_row,
    parse_osi,
)

_ET = ZoneInfo("America/New_York")
_HKT = ZoneInfo("Asia/Hong_Kong")


def _rows(raw):
    return [
        dict(zip(("time_key", "open", "high", "low", "close", "volume"), r))
        for r in raw
    ]


def _starts(bars, tz):
    return [
        datetime.fromtimestamp(b["time"] / 1000, tz).strftime("%H:%M") for b in bars
    ]


def test_us_hourly_bars_start_at_their_open_and_the_last_half_hour_keeps_its_start():
    rows = _rows(
        [
            ("2026-10-23 10:30:00", 349.115, 351.73, 346.9568, 347.31, 9249219),
            ("2026-10-23 11:30:00", 347.29, 348.41, 346.43, 348.2275, 4793169),
            ("2026-10-23 15:30:00", 346.3, 347.08, 346.02, 346.64, 2098799),
            ("2026-10-23 16:00:00", 346.62, 347.93, 346.57, 347.725, 8746531),
        ]
    )
    bars = bars_from_klines(rows, _ET, "1hour")
    assert _starts(bars, _ET) == ["09:30", "10:30", "14:30", "15:30"]
    assert bars[-1]["close"] == 347.725 and bars[-1]["volume"] == 8746531


def test_hk_hourly_bar_after_lunch_starts_at_the_reopen():
    rows = _rows(
        [
            ("2026-10-23 11:30:00", 380.4, 382.8, 379.6, 382.0, 3862100),
            ("2026-10-23 12:00:00", 381.8, 382.2, 380.2, 380.6, 1597100),
            ("2026-10-23 14:00:00", 380.8, 386.8, 380.6, 385.6, 4911000),
        ]
    )
    assert _starts(bars_from_klines(rows, _HKT, "1hour"), _HKT) == [
        "10:30",
        "11:30",
        "13:00",
    ]


def test_futu_codes():
    assert futu_code("AAPL")[0] == "US.AAPL"
    assert futu_code("BRK-B")[0] == "US.BRK.B"
    assert futu_code("0700.HK")[0] == "HK.00700"
    assert futu_code("600519.SS")[0] == "SH.600519"
    assert futu_code("HSI", is_index=True)[0] == "HK.800000"
    with pytest.raises(ValueError):
        futu_code("GSPC", is_index=True)  # OpenD serves no US index


def test_uw_contract_row_and_range_filter():
    row = contract_row(
        {
            "option_symbol": "AAPL261120P00300000",
            "open_interest": 21641,
            "implied_volatility": "0.3545965494177623",
            "delta": -0.057289565847976,
        }
    )
    assert row["ticker"] == "O:AAPL261120P00300000"
    assert (
        row["underlying_ticker"],
        row["contract_type"],
        row["strike_price"],
        row["expiration_date"],
    ) == (
        "AAPL",
        "put",
        300.0,
        "2026-11-20",
    )
    assert _in_range(
        row, {"expiration_date_gte": "2026-11-20", "strike_price_lte": 300}
    )
    assert not _in_range(row, {"strike_price_gte": 300.5})
    assert parse_osi("SPXW261120C05800000") == ("SPXW", "2026-11-20", "call", 5800.0)


def test_us_snapshot_carries_both_extended_sessions_on_their_own_anchors():
    from src.data_client.futu.data_source import snapshot_row

    # OpenD snapshot for US.AAPL after the 2026-10-23 session.
    row = {
        "last_price": 347.725, "prev_close_price": 348.22, "update_time": "2026-10-23 20:02:18.212",
        "pre_price": 349.70, "pre_change_val": 1.48, "pre_change_rate": 0.425,
        "after_price": 347.61, "after_change_val": -0.115, "after_change_rate": -0.033,
    }
    snap = snapshot_row(row, "AAPL", _ET, "closed")
    assert (snap["price"], snap["regular_close"], snap["last_trade_price"]) == (347.725, 347.725, 347.61)
    assert (snap["early_trading_change"], snap["late_trading_change"]) == (1.48, -0.115)
    assert snap["regular_only"] is False
    assert snapshot_row(row, "AAPL", _ET, "pre")["last_trade_price"] == 349.70
    hk = snapshot_row({**row, "pre_price": "N/A"}, "0700.HK", _HKT, None)
    assert hk["regular_only"] is True and hk["early_trading_change"] is None


@pytest.mark.asyncio
async def test_uw_four_hour_option_bars_keep_et_boundaries_in_winter():
    # O:AAPL260116C00250000's real UW minutes at 11:31 and 12:26 ET on
    # 2026-01-06 (EST), read 2026-10-25. UTC-anchored buckets put both in 11:00.
    uw = UnusualWhalesIntelSource("test")

    async def get(path, params=None):
        return {"data": [
            {"start_time": "2026-01-06T16:31:00Z", "open": "13.60", "high": "13.60",
             "low": "13.51", "close": "13.51", "volume_ask_side": 256},
            {"start_time": "2026-01-06T17:26:00Z", "open": "13.82", "high": "13.82",
             "low": "13.82", "close": "13.82", "volume_bid_side": 1},
        ]}

    uw._get = get
    out = await uw.get_options_ohlcv("O:AAPL260116C00250000", "2026-01-06", "2026-01-06", "4hour")
    assert _starts(out, _ET) == ["08:00", "12:00"]
    assert [b["volume"] for b in out] == [256, 1]
    await uw.close()


@pytest.mark.asyncio
async def test_uw_weekly_option_bar_keeps_the_window_and_stamps_monday():
    # O:AAPL261120P00300000's real UW daily rows for 2026-10-19..23, read 2026-10-25.
    rows = [
        ("2026-10-19", "1.52", "1.52", "1.17", "1.17", 1481),
        ("2026-10-20", "1.40", "1.40", "1.18", "1.30", 893),
        ("2026-10-21", "1.25", "1.26", "0.93", "0.95", 13616),
        ("2026-10-22", "1.00", "1.21", "0.87", "0.89", 3305),
        ("2026-10-23", "0.82", "1.00", "0.80", "0.85", 6508),
    ]
    uw = UnusualWhalesIntelSource("test")

    async def get(path, params=None):
        keys = ("date", "open_price", "high_price", "low_price", "last_price", "volume")
        return {"chains": [dict(zip(keys, r)) for r in rows]}

    uw._get = get
    (bar,) = await uw.get_options_ohlcv("O:AAPL261120P00300000", "2026-10-21", "2026-10-23", "1week")
    assert datetime.fromtimestamp(bar["time"] / 1000, _ET).date().isoformat() == "2026-10-19"
    assert (bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]) == (
        1.25, 1.26, 0.80, 0.85, 23429,
    )
    await uw.close()


@pytest.mark.parametrize("start", [None, "2024-01-02"])
def test_futu_klines_always_send_an_end(start):
    # OpenD reads a missing end as start + 365 days.
    from src.data_client.futu.data_source import FutuDataSource

    sent = {}

    class _Frame:
        def to_dict(self, _):
            return []

    def call(method, code, **kw):
        sent.update(kw)
        return _Frame(), None

    src = FutuDataSource()
    src._call = call
    src._klines("US.AAPL", "K_DAY", start, None, _ET)
    today = datetime.now(_ET).date()
    assert sent["end"] == today.isoformat()
    assert sent["start"] == (start or (today - timedelta(days=730)).isoformat())
