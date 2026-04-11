from pathlib import Path

import pandas as pd

from druck.data import _cache_key, fetch_prices


def test_cache_key_is_stable():
    key1 = _cache_key(["SPY", "QQQ"], "2024-01-01", "2024-01-03")
    key2 = _cache_key(["QQQ", "SPY"], "2024-01-01", "2024-01-03")
    assert key1 == key2
    assert key1.startswith("px_")


def test_fetch_prices_uses_cache_when_available(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    expected = pd.DataFrame({"SPY": [1.0, 2.0]}, index=pd.to_datetime(["2024-01-01", "2024-01-02"]))
    cache_key = _cache_key(["SPY"], "2024-01-01", "2024-01-03")
    expected.to_csv(Path(cache_dir) / cache_key)

    result = fetch_prices(["SPY"], "2024-01-01", "2024-01-03", cache_dir=str(cache_dir), use_cache=True)
    assert list(result.columns) == ["SPY"]
    assert len(result) == 2
