import os
from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import DBT_LOG_PATH_ENVVAR


dbt_log_path = "/tmp/dbt_logs"
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

profile_config = ProfileConfig(
    profiles_yml_filepath=f"{os.environ['AIRFLOW_REPO_BASE']}/dbt/mgi/profiles.yml",
    profile_name="mgi",
    target_name="prod",
)

mgi_cosmos_dag = DbtDag(
    project_config=ProjectConfig(f"{os.environ['AIRFLOW_REPO_BASE']}/dbt/mgi"),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_REPO_BASE']}/.local/bin/dbt",
    ),
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="mgi_cosmos_dag",
    default_args={"retries": 2},
)
