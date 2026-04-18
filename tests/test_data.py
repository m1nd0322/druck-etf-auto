from pathlib import Path

import pandas as pd

from druck.data import _cache_key, fetch_prices, make_universe


def test_cache_key_is_stable():
    key1 = _cache_key(["SPY", "QQQ"], "2024-01-01", "2024-01-03")
    key2 = _cache_key(["QQQ", "SPY"], "2024-01-01", "2024-01-03")
    assert key1 == key2
    assert key1.startswith("px_")


def test_fetch_prices_uses_cache_when_available(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    expected = pd.DataFrame({"SPY": [1.0, 2.0]}, index=pd.to_datetime(["2024-01-01", "2024-01-02"]))
    cache_key = _cache_key(["SPY"], "2024-01-01", "2024-01-03")
    expected.to_csv(Path(cache_dir) / cache_key)

    result = fetch_prices(["SPY"], "2024-01-01", "2024-01-03", cache_dir=str(cache_dir), use_cache=True)
    assert list(result.columns) == ["SPY"]
    assert len(result) == 2


def test_make_universe_combines_factor_sector_and_country_sleeves():
    cfg = {
        "universe": {
            "kr": {"auto_generate": False, "tickers": []},
            "us": {
                "tickers": ["SPY", "QQQ"],
                "factor_tickers": ["MTUM", "QUAL"],
                "sector_tickers": ["XLK", "XLF"],
                "country_tickers": ["EEM", "EWJ"],
            },
        }
    }
    u = make_universe(cfg)
    assert "SPY" in u.us
    assert "MTUM" in u.us
    assert "XLK" in u.us
    assert "EEM" in u.us
    assert u.us_factor == ["MTUM", "QUAL"]
    assert u.us_sector == ["XLK", "XLF"]
    assert u.us_country == ["EEM", "EWJ"]


def test_fetch_prices_merges_shared_data_with_provider_fallback(monkeypatch, tmp_path):
    idx = pd.to_datetime(["2024-01-01", "2024-01-02"])
    shared_close = pd.DataFrame({"SPY": [100.0, 101.0]}, index=idx)
    fallback_close = pd.DataFrame({"QQQ": [200.0, 202.0]}, index=idx)

    monkeypatch.setattr("druck.data._HAS_SHARED_DATA", True)
    monkeypatch.setattr("druck.data.load_tickers", lambda tickers, start, end: {"Close": shared_close})
    monkeypatch.setattr("druck.data.fetch_prices_yf", lambda tickers, start, end: fallback_close if tickers == ["QQQ"] else pd.DataFrame())
    monkeypatch.setattr("druck.data.fetch_prices_fdr", lambda tickers, start, end: pd.DataFrame())

    result = fetch_prices(["SPY", "QQQ"], "2024-01-01", "2024-01-03", prefer="auto", cache_dir=str(tmp_path), use_cache=False)
    assert list(result.columns) == ["SPY", "QQQ"]
    assert float(result.loc[idx[0], "SPY"]) == 100.0
    assert float(result.loc[idx[1], "QQQ"]) == 202.0
