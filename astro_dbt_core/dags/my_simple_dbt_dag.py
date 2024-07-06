
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.datasets import Dataset
from datetime import datetime

stocks_dataset = Dataset("duckdb://usr/local/airflow/database/parking_violations_database.duckdb")

profile_config = ProfileConfig(
    profile_name="dbt_app",
    target_name="dev",
    profiles_yml_filepath="/usr/local/airflow/dags/dbt/dbt_app/profiles.yml"
)
dbt_task_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/dbt_app"),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path= "/usr/local/airflow/dbt_venv/bin/dbt"),
    schedule=[stocks_dataset],
    start_date=datetime(2024, 5, 1),
    default_args={"retries": 0},
    dag_id="daily_stocks_dag",
)