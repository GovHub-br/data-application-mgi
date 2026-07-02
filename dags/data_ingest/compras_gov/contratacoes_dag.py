import logging
import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from mgi.cliente_compras_gov import ClienteComprasGov
from mgi.cliente_postgres import ClientPostgresDB
from mgi.helpers.postgres_helpers import get_postgres_conn

SCHEMA = "compras_gov"
PAGE_SIZE = 500
BLOCK_SIZE = 15
MODALIDADES = [3, 5, 6, 7]  # Concorrência Eletrônica, Pregão, Dispensa, Inexigibilidade

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


def _get_intervalo(context: object) -> tuple[str, str]:
    data_inicial = str(context["data_interval_start"].date())  # type: ignore[index]
    data_final = str(context["data_interval_end"].date())  # type: ignore[index]
    return data_inicial, data_final


@dag(
    dag_id="contratacoes_dag",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "contratacoes", "raw"],
)
def contratacoes_dag() -> None:
    @task
    def get_page_starts_contratacoes(modalidade: int, **context: object) -> list[int]:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET",
            "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
            params={
                "dataPublicacaoPncpInicial": data_inicial,
                "dataPublicacaoPncpFinal": data_final,
                "codigoModalidade": modalidade,
                "pagina": 1,
                "tamanhoPagina": PAGE_SIZE,
            },
        )
        total = resp.get("totalPaginas", 1) if isinstance(resp, dict) else 1
        logging.info("[contratacoes] modalidade=%s %s→%s total_paginas=%s", modalidade, data_inicial, data_final, total)
        return list(range(1, total + 1, BLOCK_SIZE))

    @task
    def fetch_block_contratacoes(pagina_inicio: int, modalidade: int, **context: object) -> dict:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            time.sleep(1)
            _, resp = api.request(
                "GET",
                "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
                params={
                    "dataPublicacaoPncpInicial": data_inicial,
                    "dataPublicacaoPncpFinal": data_final,
                    "codigoModalidade": modalidade,
                    "pagina": pagina,
                    "tamanhoPagina": PAGE_SIZE,
                },
            )
            if not isinstance(resp, dict):
                break
            data = [r for r in resp.get("resultado", []) if r is not None]
            api_total = resp.get("totalRegistros", 0)
            if not data:
                break
            db.insert_data(
                _stamp(data), "raw_contratacoes",
                primary_key=["idcompra"], conflict_fields=["idcompra"], schema=SCHEMA,
            )
            ingeridos += len(data)
            if resp.get("paginasRestantes", 0) == 0:
                break
        return {"ingeridos": ingeridos, "api_total": api_total}

    @task
    def validate_contratacoes(results: list[dict], modalidade: int) -> None:
        total_ingerido = sum(r["ingeridos"] for r in results)
        api_total = results[0]["api_total"] if results else 0
        if total_ingerido != api_total:
            logging.warning(
                "[contratacoes] modalidade=%s Divergência: ingeridos=%s api_total=%s",
                modalidade, total_ingerido, api_total,
            )
        else:
            logging.info("[contratacoes] modalidade=%s Validação OK: ingeridos=%s", modalidade, total_ingerido)

    # Modalidades sequenciais: m3 → m5 → m6 → m7
    prev_val = None
    for modalidade in MODALIDADES:
        starts = get_page_starts_contratacoes.override(
            task_id=f"get_page_starts_m{modalidade}"
        )(modalidade=modalidade)
        results = fetch_block_contratacoes.override(
            task_id=f"fetch_block_m{modalidade}"
        ).partial(modalidade=modalidade).expand(pagina_inicio=starts)
        val = validate_contratacoes.override(
            task_id=f"validate_m{modalidade}"
        )(results=results, modalidade=modalidade)

        if prev_val is not None:
            prev_val >> starts
        prev_val = val


contratacoes_dag()
