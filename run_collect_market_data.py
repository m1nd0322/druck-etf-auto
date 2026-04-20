from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from druck.data import fetch_prices, get_date_range
from druck.market_data import ensure_market_data_layout, safe_listing, write_parquet, write_timeseries_parquet


def _normalize_symbol_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c) for c in out.columns]
    return out


def _listing_fdr(code: str) -> pd.DataFrame:
    import FinanceDataReader as fdr

    return _normalize_symbol_frame(fdr.StockListing(code))


def _fetch_and_store_prices(tickers: list[str], start: str, end: str, path: Path, prefer: str) -> dict:
    px = fetch_prices(tickers, start, end, prefer=prefer, cache_dir='.cache', use_cache=True)
    if px.empty:
        return {"path": str(path), "rows": 0, "columns": 0}
    write_timeseries_parquet(px, path)
    return {"path": str(path), "rows": int(len(px)), "columns": int(len(px.columns))}


def main() -> int:
    parser = argparse.ArgumentParser(description='Collect FinanceDataReader market datasets to parquet')
    parser.add_argument('--root', default='.', help='project root')
    parser.add_argument('--lookback-years', type=int, default=3)
    parser.add_argument('--prices-limit', type=int, default=50, help='smoke-run ticker cap per group, 0 means no cap')
    parser.add_argument('--full', action='store_true', help='collect full price groups without cap')
    args = parser.parse_args()

    layout = ensure_market_data_layout(args.root)
    start, end = get_date_range(args.lookback_years)

    listings = {
        'krx_kospi': ('KOSPI', layout.listings_root / 'krx_kospi.parquet'),
        'krx_kosdaq': ('KOSDAQ', layout.listings_root / 'krx_kosdaq.parquet'),
        'krx_delisting': ('KRX-DELISTING', layout.listings_root / 'krx_delisting.parquet'),
        'krx_administrative': ('KRX-ADMINISTRATIVE', layout.listings_root / 'krx_administrative.parquet'),
        'krx_marcap': ('KRX-MARCAP', layout.listings_root / 'krx_marcap.parquet'),
        'us_nasdaq': ('NASDAQ', layout.listings_root / 'us_nasdaq.parquet'),
        'us_sp500': ('S&P500', layout.listings_root / 'us_sp500.parquet'),
        'kr_etf': ('ETF/KR', layout.listings_root / 'kr_etf.parquet'),
        'us_etf': ('ETF/US', layout.listings_root / 'us_etf.parquet'),
    }

    listing_summary: dict[str, dict] = {}
    listing_frames: dict[str, pd.DataFrame] = {}
    for key, (code, out_path) in listings.items():
        df = safe_listing(lambda code=code: _listing_fdr(code))
        listing_frames[key] = df
        if not df.empty:
            write_parquet(df, out_path)
        listing_summary[key] = {"path": str(out_path), "rows": int(len(df)), "columns": int(len(df.columns)) if not df.empty else 0}

    def extract_tickers(df: pd.DataFrame, suffix_ks: bool = False) -> list[str]:
        if df.empty:
            return []
        code_col = next((c for c in ['Symbol', 'Code', '종목코드', 'code'] if c in df.columns), None)
        if not code_col:
            return []
        vals = [str(v).strip() for v in df[code_col].dropna().tolist()]
        vals = [v.zfill(6) if suffix_ks and v.isdigit() else v for v in vals]
        vals = [f'{v}.KS' if suffix_ks and not v.endswith('.KS') else v for v in vals]
        return list(dict.fromkeys([v for v in vals if v]))

    def maybe_cap(items: list[str]) -> list[str]:
        if args.full or args.prices_limit <= 0:
            return items
        return items[: args.prices_limit]

    kr_stock_tickers = maybe_cap(extract_tickers(listing_frames['krx_kospi'], suffix_ks=True) + extract_tickers(listing_frames['krx_kosdaq'], suffix_ks=True))
    kr_etf_tickers = maybe_cap(extract_tickers(listing_frames['kr_etf'], suffix_ks=True))
    us_stock_tickers = maybe_cap(extract_tickers(listing_frames['us_nasdaq']))
    us_etf_tickers = maybe_cap(extract_tickers(listing_frames['us_etf']))

    price_summary = {
        'kr_stocks': _fetch_and_store_prices(kr_stock_tickers, start, end, layout.prices_root / 'kr_stocks.parquet', prefer='fdr'),
        'kr_etfs': _fetch_and_store_prices(kr_etf_tickers, start, end, layout.prices_root / 'kr_etfs.parquet', prefer='fdr'),
        'us_stocks': _fetch_and_store_prices(us_stock_tickers, start, end, layout.prices_root / 'us_stocks.parquet', prefer='yf'),
        'us_etfs': _fetch_and_store_prices(us_etf_tickers, start, end, layout.prices_root / 'us_etfs.parquet', prefer='yf'),
    }

    index_groups = {
        'kr_indexes': ['KS11', 'KQ11', 'KS200', 'KRX100'],
        'us_indexes': ['^GSPC', '^IXIC', '^DJI', '^RUT', '^VIX'],
    }
    index_summary = {
        key: _fetch_and_store_prices(maybe_cap(tickers), start, end, layout.indexes_root / f'{key}.parquet', prefer='auto')
        for key, tickers in index_groups.items()
    }

    summary = {
        'lookback_years': args.lookback_years,
        'full': bool(args.full),
        'prices_limit': int(args.prices_limit),
        'listings': listing_summary,
        'prices': price_summary,
        'indexes': index_summary,
    }
    summary_path = layout.metadata_root / 'market_data_collection_summary.json'
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
