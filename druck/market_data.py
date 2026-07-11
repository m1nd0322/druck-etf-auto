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

    @property
    def runs_root(self) -> Path:
        return self.root / "market_data" / "runs"


def ensure_market_data_layout(root: str | Path) -> MarketDataLayout:
    storage = ensure_storage_layout(root)
    layout = MarketDataLayout(storage.parquet_root)
    layout.listings_root.mkdir(parents=True, exist_ok=True)
    layout.prices_root.mkdir(parents=True, exist_ok=True)
    layout.indexes_root.mkdir(parents=True, exist_ok=True)
    layout.metadata_root.mkdir(parents=True, exist_ok=True)
    layout.runs_root.mkdir(parents=True, exist_ok=True)
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


def merge_timeseries(existing_path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    def _normalize_index(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if 'date' in out.columns:
            out['date'] = pd.to_datetime(out['date'])
            out = out.set_index('date')
        else:
            out.index = pd.to_datetime(out.index)
        out.index.name = 'date'
        return out

    if new_df is None or new_df.empty:
        if existing_path.exists():
            loaded = pd.read_parquet(existing_path)
            return _normalize_index(loaded)
        return pd.DataFrame()

    merged = _normalize_index(new_df)
    if existing_path.exists():
        old = pd.read_parquet(existing_path)
        old = _normalize_index(old)
        merged = merged.combine_first(old)
    merged = merged.sort_index()
    merged = merged.loc[:, ~merged.columns.duplicated()]
    return merged


def safe_listing(fetcher: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    try:
        df = fetcher()
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def chunked(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [items]
    return [items[i:i + size] for i in range(0, len(items), size)]
