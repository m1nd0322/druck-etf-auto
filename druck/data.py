from __future__ import annotations
import hashlib
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional, Tuple
import pandas as pd

# 공유 시장 데이터 로더
_SHARED_DATA_IMPORT_ERROR = None
load_tickers = None
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), '../../data')))
try:
    from load_market_data import load_tickers
    _HAS_SHARED_DATA = True
except ImportError as exc:
    _HAS_SHARED_DATA = False
    _SHARED_DATA_IMPORT_ERROR = exc

@dataclass
class Universe:
    kr: List[str]
    us: List[str]
    us_factor: List[str] | None = None
    us_sector: List[str] | None = None
    us_country: List[str] | None = None

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def _cache_key(tickers: List[str], start: str, end: str) -> str:
    raw = "|".join(sorted(tickers)) + f"::{start}::{end}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"px_{digest}_{start}_{end}.csv"


def generate_kr_etf_universe(whitelist: Optional[List[str]]=None, blacklist: Optional[List[str]]=None) -> List[str]:
    whitelist = whitelist or []
    blacklist = set(blacklist or [])
    tickers: List[str] = []
    try:
        import FinanceDataReader as fdr
        try:
            df = fdr.StockListing('ETF/KR')
        except Exception:
            df = fdr.StockListing('KRX')
        code_col = None
        for c in ['Symbol','Code','종목코드','code']:
            if c in df.columns:
                code_col = c; break
        if code_col:
            raw = df[code_col].astype(str).tolist()
            tickers = [t.zfill(6)+'.KS' if not t.endswith('.KS') else t for t in raw]
    except Exception:
        pass
    if not tickers:
        try:
            from pykrx import stock
            raw = stock.get_etf_ticker_list()
            tickers = [t+'.KS' for t in raw]
        except Exception:
            pass
    tickers = [t for t in tickers if t not in blacklist]
    out = list(dict.fromkeys(tickers + whitelist))
    return out

def make_universe(cfg: dict) -> Universe:
    ucfg = cfg['universe']
    kr_cfg = ucfg['kr']
    if kr_cfg.get('auto_generate', True):
        kr = generate_kr_etf_universe(
            whitelist=kr_cfg.get('whitelist_tickers', []),
            blacklist=kr_cfg.get('blacklist_tickers', []),
        )
    else:
        kr = list(dict.fromkeys(kr_cfg.get('tickers', []) + kr_cfg.get('whitelist_tickers', [])))

    us_cfg = ucfg['us']
    us_base = us_cfg.get('tickers', [])
    us_factor = us_cfg.get('factor_tickers', [])
    us_sector = us_cfg.get('sector_tickers', [])
    us_country = us_cfg.get('country_tickers', [])
    us = list(dict.fromkeys(us_base + us_factor + us_sector + us_country))
    return Universe(
        kr=kr,
        us=us,
        us_factor=list(dict.fromkeys(us_factor)),
        us_sector=list(dict.fromkeys(us_sector)),
        us_country=list(dict.fromkeys(us_country)),
    )

def fetch_prices_yf(tickers: List[str], start: str, end: str) -> pd.DataFrame:
    import yfinance as yf
    df = yf.download(tickers, start=start, end=end, auto_adjust=True, progress=False)['Close']
    if isinstance(df, pd.Series):
        df = df.to_frame()
    return df.dropna(how='all')

def fetch_prices_fdr(tickers: List[str], start: str, end: str) -> pd.DataFrame:
    import FinanceDataReader as fdr
    out={}
    for t in tickers:
        try:
            s=fdr.DataReader(t, start, end)
            if s is None or len(s)==0:
                continue
            col = 'Close' if 'Close' in s.columns else ('close' if 'close' in s.columns else None)
            if col:
                out[t]=s[col]
        except Exception:
            continue
    if not out:
        return pd.DataFrame()
    return pd.DataFrame(out).dropna(how='all')

def fetch_prices(tickers: List[str], start: str, end: str, prefer: str='auto', cache_dir: Optional[str]=None, use_cache: bool=True) -> pd.DataFrame:
    tickers=[t for t in tickers if t]
    if not tickers:
        return pd.DataFrame()
    cache_path=None
    if cache_dir:
        _ensure_dir(cache_dir)
        key=_cache_key(tickers, start, end)
        cache_path=os.path.join(cache_dir,key)
    if use_cache and cache_path and os.path.exists(cache_path):
        try:
            return pd.read_csv(cache_path, index_col=0, parse_dates=True)
        except Exception:
            pass
    # 공유 Parquet 데이터에서 먼저 시도 (부분 히트 지원)
    shared_df = pd.DataFrame()
    missing_tickers = list(tickers)
    if _HAS_SHARED_DATA:
        try:
            shared = load_tickers(tickers, start, end)
            if 'Close' in shared and not shared['Close'].empty:
                found = [t for t in tickers if t in shared['Close'].columns]
                if found:
                    shared_df = shared['Close'][found].sort_index().dropna(how='all')
                    missing_tickers = [t for t in tickers if t not in found]
        except Exception as exc:
            print(f"[data] shared data load failed, falling back to providers: {exc}")
    elif _SHARED_DATA_IMPORT_ERROR is not None:
        print(f"[data] shared data loader unavailable, falling back to providers: {_SHARED_DATA_IMPORT_ERROR}")
    # 공유 데이터에 없는 티커만 다운로드
    dfs=[]
    if missing_tickers:
        if prefer in ('yf','auto'):
            try: dfs.append(fetch_prices_yf(missing_tickers,start,end))
            except Exception: pass
        if prefer in ('fdr','auto'):
            try: dfs.append(fetch_prices_fdr(missing_tickers,start,end))
            except Exception: pass
    if not dfs and shared_df.empty:
        raise RuntimeError('No data provider worked.')
    df = shared_df
    for d in dfs:
        df = df.combine_first(d) if not df.empty else d
    df=df.sort_index().dropna(how='all')
    ordered_cols = [t for t in tickers if t in df.columns]
    if ordered_cols:
        df = df.reindex(columns=ordered_cols)
    if cache_path:
        try: df.to_csv(cache_path)
        except Exception: pass
    return df

def get_date_range(lookback_years: int) -> Tuple[str,str]:
    end = date.today()
    start = end - timedelta(days=365*int(lookback_years))
    return str(start), str(end)
