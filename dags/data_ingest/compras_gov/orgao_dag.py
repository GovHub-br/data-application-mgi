import logging
import time
from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

from mgi.cliente_compras_gov import ClienteComprasGov
from mgi.cliente_postgres import ClientPostgresDB
from mgi.helpers.postgres_helpers import get_postgres_conn

SCHEMA = "compras_gov"
PAGE_SIZE = 500
BLOCK_SIZE = 15

ENDPOINT = "/modulo-uasg/2_consultarOrgao"
PARAMS = {"statusOrgao": "true"}
TABLE = "raw_orgao"
PK = ["codigoorgao"]

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
    dag_id="orgao_dag",
    schedule="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "uasg", "raw"],
)
def orgao_dag() -> None:
    @task
    def get_page_starts() -> list[int]:
        api = ClienteComprasGov()
        _, resp = api.request("GET", ENDPOINT, params={**PARAMS, "pagina": 1, "tamanhoPagina": PAGE_SIZE})
        total = resp.get("totalPaginas", 1) if isinstance(resp, dict) else 1
        logging.info("[%s] Total de páginas: %s", ENDPOINT, total)
        return list(range(1, total + 1, BLOCK_SIZE))

    @task
    def fetch_block(pagina_inicio: int) -> dict:
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            time.sleep(1)
            _, resp = api.request("GET", ENDPOINT, params={**PARAMS, "pagina": pagina, "tamanhoPagina": PAGE_SIZE})
            if not isinstance(resp, dict):
                break
            data = [r for r in resp.get("resultado", []) if r is not None]
            api_total = resp.get("totalRegistros", 0)
            if not data:
                break
            db.insert_data(_stamp(data), TABLE, primary_key=PK, conflict_fields=PK, schema=SCHEMA)
            ingeridos += len(data)
            if resp.get("paginasRestantes", 0) == 0:
                break
        return {"ingeridos": ingeridos, "api_total": api_total}

    @task
    def validate(results: list[dict]) -> None:
        total_ingerido = sum(r["ingeridos"] for r in results)
        api_total = results[0]["api_total"] if results else 0
        if total_ingerido != api_total:
            logging.warning(
                "[%s] Divergência: ingeridos=%s api_total=%s", ENDPOINT, total_ingerido, api_total
            )
        else:
            logging.info("[%s] Validação OK: ingeridos=%s", ENDPOINT, total_ingerido)

    trigger_contratos = TriggerDagRunOperator(
        task_id="trigger_contratos",
        trigger_dag_id="contratos_dag",
        wait_for_completion=False,
    )

    starts = get_page_starts()
    results = fetch_block.expand(pagina_inicio=starts)
    validate(results) >> trigger_contratos


orgao_dag()
