"""
Markets Data Hub - Airflow DAG
Complete ETL orchestration for market risk analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=4),
}

# DAG definition
dag = DAG(
    'markets_data_hub_etl',
    default_args=default_args,
    description='Market Risk ETL - Bronze → Silver → Gold',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['market-risk', 'databricks', 'production'],
)

# Databricks cluster configuration
databricks_cluster_config = {
    'spark_version': '13.3.x-scala2.12',
    'node_type_id': 'i3.xlarge',
    'num_workers': 2,
    'spark_conf': {
        'spark.databricks.delta.optimizeWrite.enabled': 'true',
        'spark.databricks.delta.autoCompact.enabled': 'true',
    },
}

# Start task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Bronze Layer - Ingestion
with TaskGroup('bronze_ingestion', tooltip='Ingest raw data', dag=dag) as bronze_group:
    
    ingest_trades = DatabricksSubmitRunOperator(
        task_id='ingest_trades',
        new_cluster=databricks_cluster_config,
        notebook_task={
            'notebook_path': '/Repos/markets-data-hub-repo/databricks/notebooks/bronze/01_ingest_trades',
        },
        dag=dag,
    )
    
    ingest_prices = DatabricksSubmitRunOperator(
        task_id='ingest_market_prices',
        new_cluster=databricks_cluster_config,
        notebook_task={
            'notebook_path': '/Repos/markets-data-hub-repo/databricks/notebooks/bronze/02_ingest_market_prices',
        },
        dag=dag,
    )
    
    ingest_positions = DatabricksSubmitRunOperator(
        task_id='ingest_positions',
        new_cluster=databricks_cluster_config,
        notebook_task={
            'notebook_path': '/Repos/markets-data-hub-repo/databricks/notebooks/bronze/03_ingest_positions',
        },
        dag=dag,
    )

# Data Quality - Bronze validation
validate_bronze = PythonOperator(
    task_id='validate_bronze_quality',
    python_callable=lambda: print("Running Great Expectations validations on Bronze layer"),
    dag=dag,
)

# Silver Layer - Transformations
with TaskGroup('silver_transformations', tooltip='Clean and normalize', dag=dag) as silver_group:
    
    normalize_trades = DatabricksSubmitRunOperator(
        task_id='normalize_trades',
        new_cluster=databricks_cluster_config,
        notebook_task={
            'notebook_path': '/Repos/markets-data-hub-repo/databricks/notebooks/silver/01_normalize_trades',
        },
        dag=dag,
    )
    
    enrich_market_data = DatabricksSubmitRunOperator(
        task_id='enrich_market_data',
        new_cluster=databricks_cluster_config,
        notebook_task={
            'notebook_path': '/Repos/markets-data-hub-repo/databricks/notebooks/silver/02_enrich_market_data',
        },
        dag=dag,
    )

# Data Quality - Silver validation
validate_silver = PythonOperator(
    task_id='validate_silver_quality',
    python_callable=lambda: print("Running Great Expectations validations on Silver layer"),
    dag=dag,
)

# Gold Layer - Risk Analytics
with TaskGroup('gold_risk_analytics', tooltip='Calculate risk metrics', dag=dag) as gold_group:
    
    calculate_var = DatabricksSubmitRunOperator(
        task_id='calculate_var',
        new_cluster=databricks_cluster_config,
        notebook_task={
            'notebook_path': '/Repos/markets-data-hub-repo/databricks/notebooks/gold/01_calculate_var',
        },
        dag=dag,
    )
    
    calculate_pnl = DatabricksSubmitRunOperator(
        task_id='calculate_pnl',
        new_cluster=databricks_cluster_config,
        notebook_task={
            'notebook_path': '/Repos/markets-data-hub-repo/databricks/notebooks/gold/03_pnl_attribution',
        },
        dag=dag,
    )
    
    exposure_analytics = DatabricksSubmitRunOperator(
        task_id='exposure_analytics',
        new_cluster=databricks_cluster_config,
        notebook_task={
            'notebook_path': '/Repos/markets-data-hub-repo/databricks/notebooks/gold/04_exposure_analytics',
        },
        dag=dag,
    )

# Data Quality - Gold validation
validate_gold = PythonOperator(
    task_id='validate_gold_quality',
    python_callable=lambda: print("Running Great Expectations validations on Gold layer"),
    dag=dag,
)

# Publish to BI layer
publish_to_bi = DummyOperator(
    task_id='publish_to_bi',
    dag=dag,
)

# SLA check and notifications
sla_check = PythonOperator(
    task_id='sla_check',
    python_callable=lambda: print("Checking SLA compliance"),
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start >> bronze_group >> validate_bronze >> silver_group >> validate_silver >> gold_group >> validate_gold >> publish_to_bi >> sla_check >> end
