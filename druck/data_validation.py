from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .data import fetch_prices
from .storage import ensure_storage_layout


@dataclass
class ProviderValidationConfig:
    tickers: list[str]
    start: str
    end: str
    providers: list[str]
    cache_dir: str = ".cache"
    output_dir: str = "data/provider_validation"
    storage_root: str = "."


def _provider_frame(provider: str, tickers: Iterable[str], start: str, end: str, cache_dir: str) -> pd.DataFrame:
    prices = fetch_prices(list(tickers), start, end, prefer=provider, cache_dir=cache_dir, use_cache=True)
    if prices.empty:
        return pd.DataFrame()
    out = []
    for ticker in prices.columns:
        series = prices[ticker].dropna()
        if series.empty:
            out.append({
                "provider": provider,
                "ticker": ticker,
                "rows": 0,
                "first_date": None,
                "last_date": None,
                "missing_ratio": 1.0,
                "latest_price": None,
            })
            continue
        out.append(
            {
                "provider": provider,
                "ticker": ticker,
                "rows": int(series.shape[0]),
                "first_date": str(series.index.min().date()),
                "last_date": str(series.index.max().date()),
                "missing_ratio": float(prices[ticker].isna().mean()),
                "latest_price": float(series.iloc[-1]),
            }
        )
    return pd.DataFrame(out)


def run_provider_validation(cfg: ProviderValidationConfig) -> dict[str, Path]:
    layout = ensure_storage_layout(cfg.storage_root)
    output_dir = layout.provider_validation_root if cfg.output_dir == "data/provider_validation" else Path(cfg.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    frames = []
    for provider in cfg.providers:
        frame = _provider_frame(provider, cfg.tickers, cfg.start, cfg.end, cfg.cache_dir)
        if not frame.empty:
            frames.append(frame)

    summary = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["provider", "ticker", "rows", "first_date", "last_date", "missing_ratio", "latest_price"]
    )

    detail_path = output_dir / "provider_validation_summary.parquet"
    summary.to_parquet(detail_path, index=False)

    pivot = pd.DataFrame()
    if not summary.empty:
        base = summary[["provider", "ticker", "latest_price", "rows", "missing_ratio"]].copy()
        pivot = base.pivot(index="ticker", columns="provider", values="latest_price").reset_index()
    pivot_path = output_dir / "provider_validation_latest_price_compare.parquet"
    pivot.to_parquet(pivot_path, index=False)

    return {
        "summary": detail_path,
        "latest_price_compare": pivot_path,
    }
