import pandas as pd

from druck.data import fetch_prices


def test_fetch_prices_uses_cache_when_available(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    expected = pd.DataFrame({"SPY": [1.0, 2.0]}, index=pd.to_datetime(["2024-01-01", "2024-01-02"]))
    cache_key = f"px_{hash(tuple(sorted(['SPY'])))}_2024-01-01_2024-01-03.csv"
    expected.to_csv(cache_dir / cache_key)

    result = fetch_prices(["SPY"], "2024-01-01", "2024-01-03", cache_dir=str(cache_dir), use_cache=True)
    assert list(result.columns) == ["SPY"]
    assert len(result) == 2
