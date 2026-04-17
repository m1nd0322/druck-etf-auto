from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd


@dataclass
class StorageLayout:
    root: Path

    @property
    def parquet_root(self) -> Path:
        return self.root / "data"

    @property
    def provider_validation_root(self) -> Path:
        return self.parquet_root / "provider_validation"

    @property
    def duckdb_root(self) -> Path:
        return self.root / "data" / "duckdb"

    @property
    def duckdb_file(self) -> Path:
        return self.duckdb_root / "research.duckdb"


def ensure_storage_layout(root: str | Path) -> StorageLayout:
    layout = StorageLayout(Path(root))
    layout.parquet_root.mkdir(parents=True, exist_ok=True)
    layout.provider_validation_root.mkdir(parents=True, exist_ok=True)
    layout.duckdb_root.mkdir(parents=True, exist_ok=True)
    return layout


def duckdb_query_examples() -> dict[str, str]:
    return {
        "provider_validation_summary": "SELECT provider, COUNT(*) AS tickers, AVG(missing_ratio) AS avg_missing FROM read_parquet('data/provider_validation/provider_validation_summary.parquet') GROUP BY 1 ORDER BY 1;",
        "latest_price_compare": "SELECT * FROM read_parquet('data/provider_validation/provider_validation_latest_price_compare.parquet') LIMIT 20;",
    }


def run_duckdb_query(root: str | Path, sql: str) -> pd.DataFrame:
    layout = ensure_storage_layout(root)
    resolved_sql = sql.replace("'data/", f"'{layout.root.as_posix()}/data/")
    con = duckdb.connect(str(layout.duckdb_file))
    try:
        return con.execute(resolved_sql).df()
    finally:
        con.close()
