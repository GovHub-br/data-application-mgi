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

ENDPOINTS = [
    ("/modulo-servico/1_consultarSecaoServico",   {"statusSecao": "true"},       "raw_secao_servico",    ["codigosecao"]),
    ("/modulo-servico/2_consultarDivisaoServico",  {"statusDivisao": "true"},     "raw_divisao_servico",  ["codigodivisao"]),
    ("/modulo-servico/3_consultarGrupoServico",    {"statusGrupo": "true"},       "raw_grupo_servico",    ["codigogrupo"]),
    ("/modulo-servico/4_consultarClasseServico",   {},                            "raw_classe_servico",   ["codigoclasse"]),
    ("/modulo-servico/5_consultarSubClasseServico",{"statusSubclasse": "true"},   "raw_subclasse_servico",["codigosubclasse"]),
]

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
    dag_id="catalogo_servico_hierarquia_dag",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "servico", "raw"],
)
def catalogo_servico_hierarquia_dag() -> None:
    @task
    def get_page_starts(endpoint: str, query_params: dict) -> list[int]:
        api = ClienteComprasGov()
        _, resp = api.request("GET", endpoint, params={**query_params, "pagina": 1, "tamanhoPagina": PAGE_SIZE})
        total = resp.get("totalPaginas", 1) if isinstance(resp, dict) else 1
        logging.info("[%s] Total de páginas: %s", endpoint, total)
        return list(range(1, total + 1, BLOCK_SIZE))

    @task
    def fetch_block(pagina_inicio: int, endpoint: str, query_params: dict, table: str, pk: list[str]) -> dict:
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            time.sleep(1)
            _, resp = api.request("GET", endpoint, params={**query_params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE})
            if not isinstance(resp, dict):
                break
            data = [r for r in resp.get("resultado", []) if r is not None]
            api_total = resp.get("totalRegistros", 0)
            if not data:
                break
            db.insert_data(_stamp(data), table, primary_key=pk, conflict_fields=pk, schema=SCHEMA)
            ingeridos += len(data)
            if resp.get("paginasRestantes", 0) == 0:
                break
        return {"ingeridos": ingeridos, "api_total": api_total}

    @task
    def validate(results: list[dict], endpoint: str) -> None:
        total_ingerido = sum(r["ingeridos"] for r in results)
        api_total = results[0]["api_total"] if results else 0
        if total_ingerido != api_total:
            logging.warning(
                "[%s] Divergência: ingeridos=%s api_total=%s", endpoint, total_ingerido, api_total
            )
        else:
            logging.info("[%s] Validação OK: ingeridos=%s", endpoint, total_ingerido)

    for endpoint, query_params, table, pk in ENDPOINTS:
        slug = table.removeprefix("raw_").replace("_", "-")
        starts = get_page_starts.override(task_id=f"get_page_starts_{slug}")(
            endpoint=endpoint, query_params=query_params
        )
        results = fetch_block.override(task_id=f"fetch_block_{slug}").partial(
            endpoint=endpoint, query_params=query_params, table=table, pk=pk
        ).expand(pagina_inicio=starts)
        validate.override(task_id=f"validate_{slug}")(results=results, endpoint=endpoint)


catalogo_servico_hierarquia_dag()
