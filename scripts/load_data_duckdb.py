
import duckdb, pathlib

BASE = pathlib.Path(__file__).resolve().parents[1]
con = duckdb.connect(str(BASE / "local.duckdb"))

# Create staging from CSV
con.execute((BASE / "sql/models/stg_trades.sql").read_text())
con.execute((BASE / "sql/models/stg_prices.sql").read_text())
# Derived
con.execute((BASE / "sql/models/positions.sql").read_text())
con.execute((BASE / "sql/models/risk_metrics.sql").read_text())

# Write validation snapshots
rc = con.execute((BASE / "sql/validations/row_counts.sql").read_text()).fetchdf()
print(rc)
