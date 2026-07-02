import os
import shutil
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import DBT_LOG_PATH_ENVVAR


# dags/mgi/dbt/mgi_cosmos_dag.py -> raiz do repo esta 3 niveis acima (parents[3]):
# parents[0]=dbt, parents[1]=mgi, parents[2]=dags, parents[3]=<raiz do repo/bundle>
REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_PATH = REPO_ROOT / "dbt" / "mgi"
DBT_EXECUTABLE_PATH = (
    os.environ.get("DBT_EXECUTABLE_PATH")
    or shutil.which("dbt")
    or str(REPO_ROOT / ".local" / "bin" / "dbt")
)

dbt_log_path = "/tmp/dbt_logs"
os.makedirs(dbt_log_path, exist_ok=True)
os.environ[DBT_LOG_PATH_ENVVAR] = dbt_log_path

profile_config = ProfileConfig(
    profiles_yml_filepath=str(DBT_PROJECT_PATH / "profiles.yml"),
    profile_name="mgi",
    target_name="prod",
)

mgi_cosmos_dag = DbtDag(
    project_config=ProjectConfig(str(DBT_PROJECT_PATH)),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    ),
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="mgi_cosmos_dag",
    default_args={"retries": 2, "queue": "mgi"},
)
