from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from .storage import ensure_storage_layout


@dataclass
class MarketDataLayout:
    root: Path

    @property
    def listings_root(self) -> Path:
        return self.root / "market_data" / "listings"

    @property
    def prices_root(self) -> Path:
        return self.root / "market_data" / "prices"

    @property
    def indexes_root(self) -> Path:
        return self.root / "market_data" / "indexes"

    @property
    def metadata_root(self) -> Path:
        return self.root / "market_data" / "metadata"


def ensure_market_data_layout(root: str | Path) -> MarketDataLayout:
    storage = ensure_storage_layout(root)
    layout = MarketDataLayout(storage.parquet_root)
    layout.listings_root.mkdir(parents=True, exist_ok=True)
    layout.prices_root.mkdir(parents=True, exist_ok=True)
    layout.indexes_root.mkdir(parents=True, exist_ok=True)
    layout.metadata_root.mkdir(parents=True, exist_ok=True)
    return layout


def write_parquet(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def write_timeseries_parquet(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    if out.index.name is None:
        out.index.name = "date"
    out.reset_index().to_parquet(path, index=False)
    return path


def safe_listing(fetcher: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    try:
        df = fetcher()
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()
