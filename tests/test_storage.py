from druck.storage import ensure_storage_layout, duckdb_query_examples


def test_ensure_storage_layout_creates_expected_dirs(tmp_path):
    layout = ensure_storage_layout(tmp_path)
    assert layout.parquet_root.exists()
    assert layout.provider_validation_root.exists()
    assert layout.duckdb_root.exists()


def test_duckdb_query_examples_contains_expected_queries():
    queries = duckdb_query_examples()
    assert "provider_validation_summary" in queries
    assert "read_parquet" in queries["provider_validation_summary"]
