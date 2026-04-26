from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from druck.data import fetch_prices, get_date_range
from druck.market_data import (
    chunked,
    ensure_market_data_layout,
    merge_timeseries,
    safe_listing,
    write_parquet,
    write_timeseries_parquet,
)


def _normalize_symbol_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c) for c in out.columns]
    return out


def _listing_fdr(code: str) -> pd.DataFrame:
    import FinanceDataReader as fdr

    return _normalize_symbol_frame(fdr.StockListing(code))


def _extract_tickers(df: pd.DataFrame, suffix_ks: bool = False) -> list[str]:
    if df.empty:
        return []
    code_col = next((c for c in ['Symbol', 'Code', '종목코드', 'code'] if c in df.columns), None)
    if not code_col:
        return []
    vals = [str(v).strip() for v in df[code_col].dropna().tolist()]
    vals = [v.zfill(6) if suffix_ks and v.isdigit() else v for v in vals]
    vals = [f'{v}.KS' if suffix_ks and not v.endswith('.KS') else v for v in vals]
    return list(dict.fromkeys([v for v in vals if v]))


def _collect_price_group(
    name: str,
    tickers: list[str],
    start: str,
    end: str,
    path: Path,
    prefer: str,
    chunk_size: int,
    runs_root: Path,
) -> dict:
    run_ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    run_dir = runs_root / run_ts
    run_dir.mkdir(parents=True, exist_ok=True)

    collected_frames: list[pd.DataFrame] = []
    found: set[str] = set()
    chunk_logs: list[dict] = []
    missing: list[str] = []
    warnings: list[dict] = []

    for idx, group in enumerate(chunked(tickers, chunk_size), start=1):
        px = fetch_prices(group, start, end, prefer=prefer, cache_dir='.cache', use_cache=True)
        hit_cols = list(px.columns) if not px.empty else []
        found.update(hit_cols)
        missed = [t for t in group if t not in hit_cols]
        missing.extend(missed)
        warning_summary = getattr(px, 'attrs', {}).get('provider_warning_summary', {}) if px is not None else {}
        if warning_summary:
            warnings.append({'chunk': idx, **warning_summary})
        if not px.empty:
            collected_frames.append(px)
        chunk_logs.append({
            'chunk': idx,
            'requested': len(group),
            'returned_columns': len(hit_cols),
            'missing_count': len(missed),
            'missing_tickers': missed,
        })

    combined = pd.DataFrame()
    for frame in collected_frames:
        combined = combined.combine_first(frame) if not combined.empty else frame
    combined = combined.sort_index() if not combined.empty else combined

    if not combined.empty:
        merged = merge_timeseries(path, combined)
        write_timeseries_parquet(merged, path)
    else:
        merged = pd.DataFrame()

    found_list = [t for t in tickers if t in found]
    missing_list = [t for t in tickers if t not in found]

    write_parquet(pd.DataFrame({'ticker': found_list}), run_dir / f'{name}_found_tickers.parquet')
    write_parquet(pd.DataFrame({'ticker': missing_list}), run_dir / f'{name}_missing_tickers.parquet')
    write_parquet(pd.DataFrame(chunk_logs), run_dir / f'{name}_chunk_log.parquet')
    warning_summary = {
        'chunks_with_warnings': len(warnings),
        'issue_count': int(sum(int(w.get('issue_count', 0)) for w in warnings)),
        'counts': {},
        'messages': [],
        'tickers': {},
    }
    for warning in warnings:
        for category, count in (warning.get('counts') or {}).items():
            warning_summary['counts'][category] = warning_summary['counts'].get(category, 0) + int(count)
        for message in warning.get('messages') or []:
            if message not in warning_summary['messages']:
                warning_summary['messages'].append(message)
        for category, names in (warning.get('tickers') or {}).items():
            bucket = warning_summary['tickers'].setdefault(category, [])
            for name in names or []:
                if name not in bucket:
                    bucket.append(name)
    if warnings:
        write_parquet(pd.DataFrame(warnings), run_dir / f'{name}_provider_warnings.parquet')

    return {
        'path': str(path),
        'rows': int(len(merged)) if not merged.empty else 0,
        'columns': int(len(merged.columns)) if not merged.empty else 0,
        'requested_tickers': int(len(tickers)),
        'found_tickers': int(len(found_list)),
        'missing_tickers': int(len(missing_list)),
        'chunk_size': int(chunk_size),
        'missing_ticker_examples': missing_list[:10],
        'warning_summary': warning_summary,
        'run_dir': str(run_dir),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Collect FinanceDataReader market datasets to parquet')
    parser.add_argument('--root', default='.', help='project root')
    parser.add_argument('--lookback-years', type=int, default=3)
    parser.add_argument('--prices-limit', type=int, default=50, help='smoke-run ticker cap per group, 0 means no cap')
    parser.add_argument('--full', action='store_true', help='collect full price groups without cap')
    parser.add_argument('--kr-chunk-size', type=int, default=1)
    parser.add_argument('--us-chunk-size', type=int, default=50)
    parser.add_argument('--index-chunk-size', type=int, default=10)
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
        listing_summary[key] = {'path': str(out_path), 'rows': int(len(df)), 'columns': int(len(df.columns)) if not df.empty else 0}

    def maybe_cap(items: list[str]) -> list[str]:
        if args.full or args.prices_limit <= 0:
            return items
        return items[: args.prices_limit]

    kr_stock_tickers = maybe_cap(_extract_tickers(listing_frames['krx_kospi'], suffix_ks=True) + _extract_tickers(listing_frames['krx_kosdaq'], suffix_ks=True))
    kr_etf_tickers = maybe_cap(_extract_tickers(listing_frames['kr_etf'], suffix_ks=True))
    us_stock_tickers = maybe_cap(_extract_tickers(listing_frames['us_nasdaq']))
    us_etf_tickers = maybe_cap(_extract_tickers(listing_frames['us_etf']))

    price_summary = {
        'kr_stocks': _collect_price_group('kr_stocks', kr_stock_tickers, start, end, layout.prices_root / 'kr_stocks.parquet', prefer='fdr', chunk_size=args.kr_chunk_size, runs_root=layout.runs_root),
        'kr_etfs': _collect_price_group('kr_etfs', kr_etf_tickers, start, end, layout.prices_root / 'kr_etfs.parquet', prefer='fdr', chunk_size=args.kr_chunk_size, runs_root=layout.runs_root),
        'us_stocks': _collect_price_group('us_stocks', us_stock_tickers, start, end, layout.prices_root / 'us_stocks.parquet', prefer='yf', chunk_size=args.us_chunk_size, runs_root=layout.runs_root),
        'us_etfs': _collect_price_group('us_etfs', us_etf_tickers, start, end, layout.prices_root / 'us_etfs.parquet', prefer='yf', chunk_size=args.us_chunk_size, runs_root=layout.runs_root),
    }

    index_groups = {
        'kr_indexes': ['KS11', 'KQ11', 'KS200', 'KRX100'],
        'us_indexes': ['^GSPC', '^IXIC', '^DJI', '^RUT', '^VIX'],
    }
    index_summary = {
        key: _collect_price_group(key, maybe_cap(tickers), start, end, layout.indexes_root / f'{key}.parquet', prefer='auto', chunk_size=args.index_chunk_size, runs_root=layout.runs_root)
        for key, tickers in index_groups.items()
    }

    summary = {
        'lookback_years': args.lookback_years,
        'full': bool(args.full),
        'prices_limit': int(args.prices_limit),
        'kr_chunk_size': int(args.kr_chunk_size),
        'us_chunk_size': int(args.us_chunk_size),
        'index_chunk_size': int(args.index_chunk_size),
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
