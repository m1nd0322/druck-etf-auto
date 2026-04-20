from pathlib import Path

import pandas as pd

from druck.market_data import ensure_market_data_layout, write_parquet, write_timeseries_parquet


def test_ensure_market_data_layout_creates_expected_dirs(tmp_path):
    layout = ensure_market_data_layout(tmp_path)
    assert layout.listings_root.exists()
    assert layout.prices_root.exists()
    assert layout.indexes_root.exists()
    assert layout.metadata_root.exists()


def test_write_parquet_writes_flat_frame(tmp_path):
    path = tmp_path / 'out.parquet'
    df = pd.DataFrame({'ticker': ['AAA'], 'name': ['Alpha']})
    write_parquet(df, path)
    loaded = pd.read_parquet(path)
    assert list(loaded['ticker']) == ['AAA']


def test_write_timeseries_parquet_resets_index(tmp_path):
    path = tmp_path / 'ts.parquet'
    idx = pd.to_datetime(['2026-01-01', '2026-01-02'])
    df = pd.DataFrame({'AAA': [1.0, 2.0]}, index=idx)
    write_timeseries_parquet(df, path)
    loaded = pd.read_parquet(path)
    assert 'date' in loaded.columns
    assert 'AAA' in loaded.columns
    assert len(loaded) == 2
