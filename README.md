# Markets Data Hub — Production Market Risk Analytics Platform

A **production-grade market risk analytics platform** built with **Databricks**, **Apache Spark**, **Airflow**, and **Control-M** for end-to-end risk calculation and reporting.

![Databricks](https://img.shields.io/badge/Databricks-Delta%20Lake-red)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.8%2B-017CEE)

## 🌟 Features

### Core Capabilities
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Risk Calculations**: VaR (95%, 99%), CVaR, Greeks, P&L attribution
- **Real-time Orchestration**: Airflow + Control-M integration
- **Data Quality**: Great Expectations validation at each layer
- **Scalable Processing**: PySpark on Databricks with Delta Lake

### Risk Analytics
- **Value at Risk (VaR)**: Historical simulation method
- **Conditional VaR (CVaR)**: Expected shortfall calculations
- **Greeks**: Delta, Gamma, Vega, Theta, Rho
- **P&L Attribution**: Daily, MTD, YTD tracking
- **Exposure Analytics**: Counterparty, sector, instrument-level

## 📊 Architecture

```
Sources (Trades, Prices, Positions)
    ↓
Bronze Layer - Raw ingestion (Delta Lake)
    ↓ Data Quality Gates
Silver Layer - Normalized & enriched
    ↓ Business Rules
Gold Layer - Risk metrics (VaR, CVaR, Greeks, P&L)
    ↓
BI Layer / Risk Dashboards
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
# Clone repository
git clone <repo-url>
cd markets-data-hub-repo

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token

# Configure Airflow (local development)
docker compose -f docker/airflow-compose.yml up -d
```

### Run Pipeline

```bash
# Deploy to Databricks
databricks workspace import_dir databricks/notebooks /Repos/markets-data-hub-repo/databricks/notebooks

# Trigger Airflow DAG
# Navigate to http://localhost:8080
# Enable and trigger: markets_data_hub_etl
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
└── tests/                               # Unit & integration tests
```

## 🎯 Key Models

### Dimensions
- **dim_trades**: Trade transactions
- **dim_instruments**: Financial instruments master
- **dim_counterparties**: Counterparty master data

### Facts
- **fct_market_prices**: EOD market prices
- **fct_positions**: Daily position snapshots
- **fct_risk_metrics**: VaR, CVaR, Greeks, P&L, exposures

## 🔬 Risk Calculations

### Value at Risk (VaR)
- **Method**: Historical simulation
- **Lookback**: 252 trading days
- **Confidence Levels**: 95%, 99%
- **Calculation**: Position-level and portfolio-level

### Greeks (Derivatives)
- Delta, Gamma, Vega, Theta, Rho
- Used for options and structured products

### P&L Attribution
- Daily P&L
- Month-to-date (MTD)
- Year-to-date (YTD)

## 🧪 Testing

```bash
# Unit tests
pytest tests/ -v

# Integration tests (DuckDB)
pytest tests/integration/ -v

# Data quality tests
great-expectations checkpoint run bronze_validation
```

## 📦 Data Quality

Great Expectations validations:
- **Uniqueness**: trade_id, price_id
- **Null checks**: Critical fields
- **Range validation**: Prices, quantities, VaR values
- **Business rules**: High > Low price, VaR 99 > VaR 95

## 🔄 CI/CD

GitHub Actions workflow:
1. **Lint**: Black + flake8
2. **Test**: pytest with coverage
3. **Deploy**: Databricks notebook deployment
4. **Quality Gates**: Great Expectations validation

## 🛠️ Control-M Integration

Job-as-Code configuration:
- Databricks job specifications
- Dependencies and scheduling
- SLA definitions
- Monitoring hooks

## 📊 Monitoring & Observability

- **Structured Logging**: JSON-formatted logs
- **SLA Tracking**: 4-hour SLA with alerts
- **Data Quality Reports**: Automated validation
- **Error Handling**: Retry logic with exponential backoff

## 🚦 Deployment

### Development
```bash
# Deploy to dev environment
export ENV=dev
python scripts/deploy.py --env dev
```

### Production
```bash
# Deploy to production
export ENV=prod
python scripts/deploy.py --env prod
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

- [Architecture Guide](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)
- [Runbook](docs/runbook.md)
- [Control-M Integration](job_as_code/controlm/README.md)

## 📄 License

MIT License

---

**Built for production market risk analytics** 🚀
