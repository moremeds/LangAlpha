"""Futu K-line timing and Unusual Whales chain parsing.

Fixtures are real rows read once on 2026-10-24 for the 2026-10-23 session:
Futu OpenD hourly K-lines (qfq) and Unusual Whales option-contracts rows.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.data_client.futu.data_source import bars_from_klines, futu_code
from src.data_client.unusual_whales.market_intel_source import (
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
