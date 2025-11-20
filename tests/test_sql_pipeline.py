
import duckdb, pathlib

BASE = pathlib.Path(__file__).resolve().parents[1]

def test_pipeline_end_to_end():
    con = duckdb.connect(str(BASE / "local.duckdb"))
    # Basic presence checks
    for t in ["stg_trades","stg_prices","positions","risk_metrics"]:
        cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        assert cnt >= 1

def test_not_null_instrument():
    con = duckdb.connect(str(BASE / "local.duckdb"))
    nulls = con.execute("SELECT COUNT(*) FROM stg_trades WHERE instrument IS NULL").fetchone()[0]
    assert nulls == 0
