from pathlib import Path

import pandas as pd

from druck.market_data import chunked, ensure_market_data_layout, merge_timeseries, write_parquet, write_timeseries_parquet


def test_ensure_market_data_layout_creates_expected_dirs(tmp_path):
    layout = ensure_market_data_layout(tmp_path)
    assert layout.listings_root.exists()
    assert layout.prices_root.exists()
    assert layout.indexes_root.exists()
    assert layout.metadata_root.exists()
    assert layout.runs_root.exists()


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


def test_chunked_splits_items():
    assert chunked([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]


def test_merge_timeseries_combines_existing_and_new(tmp_path):
    path = tmp_path / 'prices.parquet'
    old = pd.DataFrame({'AAA': [1.0, 2.0]}, index=pd.to_datetime(['2026-01-01', '2026-01-02']))
    write_timeseries_parquet(old, path)
    new = pd.DataFrame({'BBB': [3.0, 4.0]}, index=pd.to_datetime(['2026-01-01', '2026-01-02']))
    merged = merge_timeseries(path, new)
    assert 'AAA' in merged.columns
    assert 'BBB' in merged.columns
    assert len(merged) == 2


def test_merge_timeseries_prefers_new_values_and_keeps_old_fallbacks(tmp_path):
    path = tmp_path / 'prices.parquet'
    dates = pd.to_datetime(['2026-01-01', '2026-01-02'])
    old = pd.DataFrame({'AAA': [1.0, 2.0]}, index=dates)
    write_timeseries_parquet(old, path)
    new = pd.DataFrame({'AAA': [10.0, float('nan')]}, index=dates)

    merged = merge_timeseries(path, new)

    assert merged.loc[dates[0], 'AAA'] == 10.0
    assert merged.loc[dates[1], 'AAA'] == 2.0
