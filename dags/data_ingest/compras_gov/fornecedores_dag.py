import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from mgi.cliente_compras_gov import ClienteComprasGov
from mgi.cliente_postgres import ClientPostgresDB
from mgi.helpers.postgres_helpers import get_postgres_conn

SCHEMA = "compras_gov"

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _stamp(records: list[dict]) -> list[dict]:
    ts = datetime.now().isoformat()
    for r in records:
        r["dt_ingest"] = ts
    return records


@dag(
    dag_id="fornecedores_dag",
    schedule="0 0 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "fornecedores", "raw"],
)
def fornecedores_dag() -> None:
    @task
    def ingest_fornecedores() -> None:
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())

        total_ingeridos = 0
        api_total = 0
        for batch, api_total in api.iter_pages(
            "/modulo-fornecedor/1_consultarFornecedor", {"ativo": "true"}
        ):
            db.insert_data(
                _stamp(batch),
                "raw_fornecedores",
                primary_key=["cnpj"],
                conflict_fields=["cnpj"],
                schema=SCHEMA,
            )
            total_ingeridos += len(batch)
        logging.info("Fornecedores: ingeridos=%s api_total=%s", total_ingeridos, api_total)

    ingest_fornecedores()


fornecedores_dag()
