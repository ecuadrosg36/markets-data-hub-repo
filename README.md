# Markets Data Hub — Market Risk Analytics Pipeline

A market risk data pipeline on **Databricks** (PySpark, Delta Lake), orchestrated with **Airflow** and **Control-M**. It ingests trades and market prices, builds positions and their market value. A historical VaR notebook is written but not wired in yet. DuckDB runs the SQL models locally for tests.

![Databricks](https://img.shields.io/badge/Databricks-Delta%20Lake-red)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.8%2B-017CEE)

## 🌟 Features

### Working today
- **Prices → positions → market value**: `01_ingest_prices.scala` cleans prices into Bronze, `02_trades_to_positions.py` sums trades into a net position per instrument (Silver), and `03_risk_metrics.sql` multiplies each position by its last close (Gold)
- **Jobs as code**: the Databricks job (`job_as_code/databricks/job.json`) and the Control-M flow (`job_as_code/controlm/flow.yaml`) run that three-step chain
- **Local runs**: DuckDB loads the sample CSVs and runs the same chain as SQL models (`sql/models`) for the tests
- **CI**: Black, flake8, and pytest with coverage on the DuckDB models; notebook deploy to Databricks runs only when credentials are set

### Written, not wired in yet
- **Historical VaR (95%, 99%)**: 252-day lookback, per position and summed per portfolio, plus exposure from market value (`databricks/notebooks/gold/01_calculate_var.py`). It reads `silver/market_prices`, but the price notebooks write Bronze only, so no step creates its input yet
- **Airflow DAG**: daily at 6 AM with a 4-hour SLA, 3 retries and email on failure, but it calls notebooks that are not in the repo (`bronze/03_ingest_positions`, `silver/02_enrich_market_data`, `gold/03_pnl_attribution`, `gold/04_exposure_analytics`)

### Designed, not built yet
The schema and the DAG already have a place for these, but nothing computes them yet:
- **CVaR, Greeks (Delta, Gamma, Vega, Theta, Rho), daily/MTD/YTD P&L and credit exposure**: columns exist in `fct_risk_metrics`, filled with nulls
- **Great Expectations checks**: rules are written in `data_quality/ge_rules.yaml`; the DAG's validation tasks are placeholders and CI only checks that the file parses
- **P&L and exposure notebooks** referenced by the DAG (`gold/03_pnl_attribution`, `gold/04_exposure_analytics`) and the **BI publish** step

## 📊 Architecture

```
Sources (Trades, Prices, Positions)
    ↓
Bronze Layer - Raw ingestion (Delta Lake)
    ↓ Data Quality Gates (rules written, not enforced yet)
Silver Layer - Normalized & enriched
    ↓ Business Rules
Gold Layer - Market value per position (VaR notebook written, not wired)
    ↓
BI Layer / Risk Dashboards (planned)
```

**Orchestration**: Apache Airflow + Control-M Job-as-Code

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Databricks workspace
- Docker (for local Airflow)
- DuckDB (for local testing)

### Installation

```bash
# 1. Clone repository
git clone <repo-url>
cd markets-data-hub-repo

# 2. Set up virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Initialize Local Database (DuckDB)
# This Step is CRITICAL for tests and local development
python scripts/load_data_duckdb.py
```

### Run and Test

```bash
# Run unit and integration tests
python -m pytest tests/ -v

# Verify Data Quality rules (YAML check)
python -c "import yaml; yaml.safe_load(open('data_quality/ge_rules.yaml'))"

# Deploy to Databricks (Cloud)
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token
databricks workspace import_dir databricks/notebooks /Repos/markets-data-hub-repo/databricks/notebooks
```

## 📁 Project Structure

```
├── airflow/
│   └── dags/
│       └── markets_data_hub_etl.py      # Main orchestration DAG
├── databricks/
│   └── notebooks/
│       ├── bronze/                       # Raw ingestion
│       │   ├── 01_ingest_trades.py
│       │   └── 02_ingest_market_prices.py
│       ├── silver/                       # Normalization
│       │   └── 01_normalize_trades.py
│       └── gold/                        # Risk analytics
│           └── 01_calculate_var.py      # VaR calculation
├── sql/
│   └── models/
│       └── market_risk_schema.sql       # Data models
├── data_quality/
│   └── ge_rules.yaml                    # Great Expectations
├── job_as_code/
│   ├── controlm/                        # Control-M flows
│   └── databricks/                      # Databricks job specs
├── config/
│   ├── config.yaml                      # Configuration
│   └── config_loader.py                 # Config management
├── utils/
│   └── logger.py                        # Structured logging
└── tests/                               # pytest on the DuckDB models
```

## 🎯 Key Models

### Dimensions
- **dim_trades**: Trade transactions
- **dim_instruments**: Financial instruments master
- **dim_counterparties**: Counterparty master data

### Facts
- **fct_market_prices**: EOD market prices
- **fct_positions**: Daily position snapshots
- **fct_risk_metrics**: VaR and exposure, filled once the VaR notebook is wired in (CVaR, Greeks and P&L columns exist, not filled yet)

## 🔬 Risk Calculations

### Value at Risk (VaR)
- **Method**: Historical simulation
- **Lookback**: 252 trading days
- **Confidence Levels**: 95%, 99%
- **Calculation**: Position-level and portfolio-level
- **Status**: the notebook is written but its Silver price input is not built yet, so it does not run in the pipeline

### Greeks and P&L (planned)
Columns for Delta, Gamma, Vega, Theta, Rho and daily/MTD/YTD P&L are in the schema; the calculations are not written yet.

## 🧪 Testing

```bash
# Load the sample data into DuckDB, then run the tests
python scripts/load_data_duckdb.py
pytest tests/ -v
```

## 📦 Data Quality

Great Expectations rules in `data_quality/ge_rules.yaml` (written, not run by the pipeline yet):
- **Uniqueness**: trade_id, price_id
- **Null checks**: Critical fields
- **Range validation**: Prices, quantities, VaR values
- **Business rules**: High > Low price, VaR 99 > VaR 95

## 🔄 CI/CD

GitHub Actions workflow:
1. **Lint**: Black + flake8
2. **Test**: pytest with coverage
3. **Deploy**: Databricks notebook deployment (skipped when credentials are not set)
4. **Quality Gates**: checks that the Great Expectations rules file is valid YAML

## 🛠️ Control-M Integration

Job-as-Code configuration:
- Databricks job specifications
- Dependencies and scheduling
- SLA definitions
- Monitoring hooks

## 📊 Monitoring & Observability

- **Structured Logging**: JSON-formatted logs (`utils/logger.py`)
- **SLA Tracking**: 4-hour SLA in the Airflow DAG, email on failure
- **Error Handling**: 3 retries, 5 minutes apart

## 🚦 Deployment

Notebooks are deployed with the Databricks CLI, as in CI:
```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token
databricks workspace import_dir databricks/notebooks /Repos/markets-data-hub-repo/databricks/notebooks --overwrite
```

## 📝 Configuration

Edit `config/config.yaml`:
```yaml
databricks:
  host: "https://your-workspace.cloud.databricks.com"
  cluster_id: "your-cluster-id"

risk:
  var_confidence_levels: [0.95, 0.99]
  lookback_days: 252

airflow:
  dag_schedule: "0 6 * * *"  # Daily at 6 AM
  sla_hours: 4
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Run tests and linting
4. Submit pull request

## 📚 Documentation

- [Architecture (Spanish)](docs/arquitectura.md)
- [Market risk basics](docs/market_risk_basics.md)
- [Operating procedures (Spanish)](docs/procedimientos_operativos.md)
- [Control-M flow](job_as_code/controlm/flow.yaml)

## 📄 License

MIT License
