from __future__ import annotations
import contextlib
import hashlib
import io
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, List, Optional, Tuple
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


@dataclass
class ProviderIssue:
    provider: str
    category: str
    detail: str
    tickers: list[str] | None = None


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

def fetch_prices_yf(tickers: List[str], start: str, end: str) -> tuple[pd.DataFrame, str]:
    import yfinance as yf
    stderr_buffer = io.StringIO()
    with contextlib.redirect_stderr(stderr_buffer):
        df = yf.download(tickers, start=start, end=end, auto_adjust=True, progress=False)['Close']
    if isinstance(df, pd.Series):
        df = df.to_frame()
    return df.dropna(how='all'), stderr_buffer.getvalue().strip()

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

def _classify_provider_issue(detail: str) -> str:
    text = (detail or "").lower()
    if any(token in text for token in ["rate limit", "ratelimit", "too many requests", "yf ratelimiterror", "429"]):
        return "rate_limit"
    if any(token in text for token in ["failed download", "possibly delisted", "no data found", "not found", "invalid", "missing"]):
        return "invalid_symbol"
    return "provider_error"


def _extract_issue_tickers(detail: str) -> list[str]:
    if not detail:
        return []
    matches = re.findall(r"['\"]([A-Z0-9^._-]+)['\"]", detail)
    return list(dict.fromkeys(matches))


def _summarize_provider_issues(issues: list[ProviderIssue]) -> dict[str, Any]:
    if not issues:
        return {}
    counts: dict[str, int] = {}
    tickers_by_category: dict[str, list[str]] = {}
    messages: list[str] = []
    for issue in issues:
        counts[issue.category] = counts.get(issue.category, 0) + 1
        bucket = tickers_by_category.setdefault(issue.category, [])
        for ticker in issue.tickers or []:
            if ticker not in bucket:
                bucket.append(ticker)
    if counts.get("rate_limit"):
        affected = ", ".join(tickers_by_category.get("rate_limit", [])[:5])
        suffix = f" ({affected})" if affected else ""
        messages.append(f"provider rate-limit detected{suffix}")
    if counts.get("invalid_symbol"):
        affected = ", ".join(tickers_by_category.get("invalid_symbol", [])[:5])
        suffix = f" ({affected})" if affected else ""
        messages.append(f"provider invalid/missing symbol noise detected{suffix}")
    if counts.get("provider_error"):
        messages.append(f"provider errors detected ({counts['provider_error']})")
    return {
        "status": "warning",
        "issue_count": len(issues),
        "counts": counts,
        "tickers": tickers_by_category,
        "messages": messages,
        "summary": "; ".join(messages),
        "issues": [
            {
                "provider": issue.provider,
                "category": issue.category,
                "detail": issue.detail,
                "tickers": issue.tickers or [],
            }
            for issue in issues
        ],
    }


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
    provider_issues: list[ProviderIssue] = []
    if _HAS_SHARED_DATA:
        try:
            shared = load_tickers(tickers, start, end)
            if 'Close' in shared and not shared['Close'].empty:
                found = [t for t in tickers if t in shared['Close'].columns]
                if found:
                    shared_df = shared['Close'][found].sort_index().dropna(how='all')
                    missing_tickers = [t for t in tickers if t not in found]
        except Exception as exc:
            provider_issues.append(ProviderIssue(provider="shared", category="provider_error", detail=str(exc), tickers=missing_tickers.copy()))
            print(f"[data] shared data load failed, falling back to providers: {exc}")
    elif _SHARED_DATA_IMPORT_ERROR is not None:
        provider_issues.append(ProviderIssue(provider="shared", category="provider_error", detail=str(_SHARED_DATA_IMPORT_ERROR), tickers=missing_tickers.copy()))
        print(f"[data] shared data loader unavailable, falling back to providers: {_SHARED_DATA_IMPORT_ERROR}")
    # 공유 데이터에 없는 티커만 다운로드
    dfs=[]
    if missing_tickers:
        if prefer in ('yf','auto'):
            try:
                yf_result = fetch_prices_yf(missing_tickers,start,end)
                if isinstance(yf_result, tuple):
                    yf_df, yf_stderr = yf_result
                else:
                    yf_df, yf_stderr = yf_result, ""
                if not yf_df.empty:
                    dfs.append(yf_df)
                if yf_stderr:
                    provider_issues.append(ProviderIssue(provider="yfinance", category=_classify_provider_issue(yf_stderr), detail=yf_stderr, tickers=_extract_issue_tickers(yf_stderr) or missing_tickers.copy()))
                missing_after_yf = [t for t in missing_tickers if t not in yf_df.columns] if not yf_df.empty else list(missing_tickers)
                if missing_after_yf and missing_after_yf != missing_tickers:
                    missing_tickers = missing_after_yf
            except Exception as exc:
                detail = str(exc)
                provider_issues.append(ProviderIssue(provider="yfinance", category=_classify_provider_issue(detail), detail=detail, tickers=_extract_issue_tickers(detail) or missing_tickers.copy()))
        if prefer in ('fdr','auto'):
            try:
                dfs.append(fetch_prices_fdr(missing_tickers,start,end))
            except Exception as exc:
                detail = str(exc)
                provider_issues.append(ProviderIssue(provider="fdr", category=_classify_provider_issue(detail), detail=detail, tickers=_extract_issue_tickers(detail) or missing_tickers.copy()))
    if not dfs and shared_df.empty:
        summary = _summarize_provider_issues(provider_issues)
        if summary.get("summary"):
            raise RuntimeError(f"No data provider worked. {summary['summary']}")
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
    try:
        df.attrs["provider_warning_summary"] = _summarize_provider_issues(provider_issues)
    except Exception:
        pass
    return df

def get_date_range(lookback_years: int) -> Tuple[str,str]:
    end = date.today()
    start = end - timedelta(days=365*int(lookback_years))
    return str(start), str(end)
