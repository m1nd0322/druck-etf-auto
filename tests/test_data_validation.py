from pathlib import Path

import pandas as pd

from druck.data_validation import ProviderValidationConfig, run_provider_validation


def test_run_provider_validation_writes_parquet_outputs(tmp_path, monkeypatch):
    idx = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])

    def fake_fetch_prices(tickers, start, end, prefer="auto", cache_dir=None, use_cache=True):
        return pd.DataFrame({ticker: [100.0, 101.0, 102.0] for ticker in tickers}, index=idx)

    monkeypatch.setattr("druck.data_validation.fetch_prices", fake_fetch_prices)

    cfg = ProviderValidationConfig(
        tickers=["SPY", "QQQ"],
        start="2024-01-01",
        end="2024-01-05",
        providers=["auto", "shared"],
        output_dir=str(tmp_path / "validation"),
        storage_root=str(tmp_path),
    )
    out = run_provider_validation(cfg)
    assert Path(out["summary"]).exists()
    assert Path(out["latest_price_compare"]).exists()

    summary = pd.read_parquet(out["summary"])
    assert set(["provider", "ticker", "rows", "missing_ratio", "latest_price"]).issubset(summary.columns)
    assert len(summary) == 4

    compare = pd.read_parquet(out["latest_price_compare"])
    assert "ticker" in compare.columns
