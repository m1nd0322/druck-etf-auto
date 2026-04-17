from pathlib import Path

import pandas as pd

from druck.storage import ensure_storage_layout, duckdb_query_examples, run_duckdb_query


def test_ensure_storage_layout_creates_expected_dirs(tmp_path):
    layout = ensure_storage_layout(tmp_path)
    assert layout.parquet_root.exists()
    assert layout.provider_validation_root.exists()
    assert layout.duckdb_root.exists()


def test_duckdb_query_examples_contains_expected_queries():
    queries = duckdb_query_examples()
    assert "provider_validation_summary" in queries
    assert "read_parquet" in queries["provider_validation_summary"]


def test_run_duckdb_query_reads_parquet(tmp_path):
    layout = ensure_storage_layout(tmp_path)
    df = pd.DataFrame({"provider": ["auto", "shared"], "missing_ratio": [0.1, 0.2]})
    out = layout.provider_validation_root / "provider_validation_summary.parquet"
    df.to_parquet(out, index=False)
    result = run_duckdb_query(tmp_path, "SELECT provider FROM read_parquet('data/provider_validation/provider_validation_summary.parquet') ORDER BY provider")
    assert list(result["provider"]) == ["auto", "shared"]
