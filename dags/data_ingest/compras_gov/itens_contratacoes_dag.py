import logging
import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from cliente_compras_gov import ClienteComprasGov
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

SCHEMA = "compras_gov"
PAGE_SIZE = 500
BLOCK_SIZE = 15

ENDPOINT = "/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133"
TABLE = "raw_itens_contratacoes"
PK = ["idcompraitem"]

default_args = {
    "owner": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _stamp(records: list[dict]) -> list[dict]:
    ts = datetime.now().isoformat()
    for r in records:
        r["dt_ingest"] = ts
    return records


def _get_intervalo(context: object) -> tuple[str, str]:
    ds: str = context["ds"]  # type: ignore[index]
    conf: dict = getattr(context.get("dag_run"), "conf", {}) or {}  # type: ignore[union-attr]
    return conf.get("data_inicial", ds), conf.get("data_final", ds)


@dag(
    dag_id="itens_contratacoes_dag",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "contratacoes", "raw"],
)
def itens_contratacoes_dag() -> None:
    @task
    def get_page_starts(**context: object) -> list[int]:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET", ENDPOINT,
            params={"dataInclusaoPncpInicial": data_inicial, "dataInclusaoPncpFinal": data_final, "pagina": 1, "tamanhoPagina": PAGE_SIZE},
        )
        total = resp.get("totalPaginas", 1) if isinstance(resp, dict) else 1
        logging.info("[%s] %s→%s total_paginas=%s", ENDPOINT, data_inicial, data_final, total)
        return list(range(1, total + 1, BLOCK_SIZE))

    @task
    def fetch_block(pagina_inicio: int, **context: object) -> dict:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            time.sleep(1)
            _, resp = api.request(
                "GET", ENDPOINT,
                params={"dataInclusaoPncpInicial": data_inicial, "dataInclusaoPncpFinal": data_final, "pagina": pagina, "tamanhoPagina": PAGE_SIZE},
            )
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
            logging.warning("[%s] Divergência: ingeridos=%s api_total=%s", ENDPOINT, total_ingerido, api_total)
        else:
            logging.info("[%s] Validação OK: ingeridos=%s", ENDPOINT, total_ingerido)

    starts = get_page_starts()
    results = fetch_block.expand(pagina_inicio=starts)
    validate(results)


itens_contratacoes_dag()
