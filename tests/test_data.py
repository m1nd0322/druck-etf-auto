from pathlib import Path

import pandas as pd
import pytest

from druck.data import _cache_key, fetch_prices, fetch_prices_fdr, make_universe, generate_kr_etf_universe, _summarize_provider_issues, ProviderIssue


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


def test_fetch_prices_falls_through_to_second_provider_when_first_provider_fails(monkeypatch, tmp_path):
    idx = pd.to_datetime(["2024-01-01", "2024-01-02"])
    fdr_close = pd.DataFrame({"SPY": [300.0, 301.0]}, index=idx)

    monkeypatch.setattr("druck.data._HAS_SHARED_DATA", False)
    monkeypatch.setattr("druck.data._SHARED_DATA_IMPORT_ERROR", None)

    def fail_yf(tickers, start, end):
        raise RuntimeError("yf failed")

    monkeypatch.setattr("druck.data.fetch_prices_yf", fail_yf)
    monkeypatch.setattr("druck.data.fetch_prices_fdr", lambda tickers, start, end: fdr_close)

    result = fetch_prices(["SPY"], "2024-01-01", "2024-01-03", prefer="auto", cache_dir=str(tmp_path), use_cache=False)
    assert list(result.columns) == ["SPY"]
    assert float(result.loc[idx[1], "SPY"]) == 301.0


def test_fetch_prices_raises_when_all_providers_fail(monkeypatch, tmp_path):
    monkeypatch.setattr("druck.data._HAS_SHARED_DATA", False)
    monkeypatch.setattr("druck.data._SHARED_DATA_IMPORT_ERROR", None)
    monkeypatch.setattr("druck.data.fetch_prices_yf", lambda tickers, start, end: (_ for _ in ()).throw(RuntimeError("yf failed")))
    monkeypatch.setattr("druck.data.fetch_prices_fdr", lambda tickers, start, end: (_ for _ in ()).throw(RuntimeError("fdr failed")))

    with pytest.raises(RuntimeError, match="No data provider worked"):
        fetch_prices(["SPY"], "2024-01-01", "2024-01-03", prefer="auto", cache_dir=str(tmp_path), use_cache=False)


def test_generate_kr_etf_universe_uses_etf_kr_listing_when_available(monkeypatch):
    class FakeFDR:
        @staticmethod
        def StockListing(market):
            assert market == "ETF/KR"
            return pd.DataFrame({"Symbol": ["069500", "357870"]})

    import sys
    monkeypatch.setitem(sys.modules, "FinanceDataReader", FakeFDR)

    tickers = generate_kr_etf_universe()
    assert tickers[:2] == ["069500.KS", "357870.KS"]


def test_generate_kr_etf_universe_falls_back_from_etf_kr_to_krx(monkeypatch):
    class FakeFDR:
        calls = []

        @classmethod
        def StockListing(cls, market):
            cls.calls.append(market)
            if market == "ETF/KR":
                raise RuntimeError("ETF/KR unavailable")
            return pd.DataFrame({"Code": ["114800"]})

    import sys
    monkeypatch.setitem(sys.modules, "FinanceDataReader", FakeFDR)

    tickers = generate_kr_etf_universe()
    assert FakeFDR.calls == ["ETF/KR", "KRX"]
    assert tickers == ["114800.KS"]


def test_generate_kr_etf_universe_falls_back_to_pykrx_and_preserves_white_black_lists(monkeypatch):
    import sys

    class BrokenFDR:
        @staticmethod
        def StockListing(market):
            raise RuntimeError("fdr unavailable")

    class FakeStock:
        @staticmethod
        def get_etf_ticker_list():
            return ["069500", "114800"]

    class FakePykrx:
        stock = FakeStock

    monkeypatch.setitem(sys.modules, "FinanceDataReader", BrokenFDR)
    monkeypatch.setitem(sys.modules, "pykrx", FakePykrx)

    tickers = generate_kr_etf_universe(whitelist=["069500.KS", "999999.KS"], blacklist=["114800.KS"])
    assert "114800.KS" not in tickers
    assert tickers == ["069500.KS", "999999.KS"]


def test_summarize_provider_issues_groups_rate_limit_and_invalid_symbol_noise():
    summary = _summarize_provider_issues(
        [
            ProviderIssue(provider="yfinance", category="rate_limit", detail="YFRateLimitError('Too Many Requests')", tickers=["XLV"]),
            ProviderIssue(provider="yfinance", category="invalid_symbol", detail="1 Failed download: ['FAKE']", tickers=["FAKE"]),
        ]
    )
    assert summary["status"] == "warning"
    assert summary["counts"]["rate_limit"] == 1
    assert summary["counts"]["invalid_symbol"] == 1
    assert "provider rate-limit detected" in summary["summary"]
    assert "provider invalid/missing symbol noise detected" in summary["summary"]
    assert summary["tickers"]["rate_limit"] == ["XLV"]
    assert summary["tickers"]["invalid_symbol"] == ["FAKE"]


def test_fetch_prices_attaches_provider_warning_summary(monkeypatch, tmp_path):
    idx = pd.to_datetime(["2024-01-01", "2024-01-02"])
    fdr_close = pd.DataFrame({"SPY": [300.0, 301.0]}, index=idx)

    monkeypatch.setattr("druck.data._HAS_SHARED_DATA", False)
    monkeypatch.setattr("druck.data._SHARED_DATA_IMPORT_ERROR", None)
    monkeypatch.setattr(
        "druck.data.fetch_prices_yf",
        lambda tickers, start, end: (_ for _ in ()).throw(RuntimeError("1 Failed download: ['SPY']: YFRateLimitError('Too Many Requests')")),
    )
    monkeypatch.setattr("druck.data.fetch_prices_fdr", lambda tickers, start, end: fdr_close)

    result = fetch_prices(["SPY"], "2024-01-01", "2024-01-03", prefer="auto", cache_dir=str(tmp_path), use_cache=False)
    summary = result.attrs.get("provider_warning_summary", {})
    assert summary["status"] == "warning"
    assert summary["counts"]["rate_limit"] == 1
    assert summary["tickers"]["rate_limit"] == ["SPY"]
    assert "provider rate-limit detected" in summary["summary"]


def test_fetch_prices_fdr_strips_ks_suffix_for_provider_lookup(monkeypatch):
    idx = pd.to_datetime(["2024-01-01", "2024-01-02"])

    class FakeFDR:
        calls = []

        @classmethod
        def DataReader(cls, ticker, start, end):
            cls.calls.append(ticker)
            return pd.DataFrame({"Close": [100.0, 101.0]}, index=idx)

    import sys
    monkeypatch.setitem(sys.modules, "FinanceDataReader", FakeFDR)

    result = fetch_prices_fdr(["005930.KS"], "2024-01-01", "2024-01-03")
    assert FakeFDR.calls == ["005930"]
    assert list(result.columns) == ["005930.KS"]


def test_fetch_prices_records_yf_partial_success_stderr_noise(monkeypatch, tmp_path):
    idx = pd.to_datetime(["2024-01-01", "2024-01-02"])
    partial_close = pd.DataFrame({"069500.KS": [100.0, 101.0]}, index=idx)
    fdr_close = pd.DataFrame({"229200.KS": [200.0, 201.0]}, index=idx)

    monkeypatch.setattr("druck.data._HAS_SHARED_DATA", False)
    monkeypatch.setattr("druck.data._SHARED_DATA_IMPORT_ERROR", None)
    monkeypatch.setattr(
        "druck.data.fetch_prices_yf",
        lambda tickers, start, end: (partial_close, '"229200.KS" invalid symbol or has no data'),
    )
    monkeypatch.setattr("druck.data.fetch_prices_fdr", lambda tickers, start, end: fdr_close if tickers == ["229200.KS"] else pd.DataFrame())

    result = fetch_prices(["069500.KS", "229200.KS"], "2024-01-01", "2024-01-03", prefer="auto", cache_dir=str(tmp_path), use_cache=False)
    assert list(result.columns) == ["069500.KS", "229200.KS"]
    summary = result.attrs.get("provider_warning_summary", {})
    assert summary["counts"]["invalid_symbol"] == 1
    assert summary["tickers"]["invalid_symbol"] == ["229200.KS"]
