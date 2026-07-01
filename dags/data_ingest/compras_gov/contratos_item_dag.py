import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from mgi.cliente_compras_gov import ClienteComprasGov
from mgi.cliente_postgres import ClientPostgresDB
from mgi.helpers.postgres_helpers import get_postgres_conn

SCHEMA = "compras_gov"

default_args = {
    "owner": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
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
    dag_id="contratos_item_dag",
    schedule="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "contratos", "raw"],
)
def contratos_item_dag() -> None:
    @task
    def get_orgaos() -> list[str]:
        db = ClientPostgresDB(get_postgres_conn())
        try:
            rows = db.execute_query(
                f"SELECT DISTINCT codigoorgao FROM {SCHEMA}.raw_orgao ORDER BY codigoorgao"
            )
        except Exception as exc:
            raise RuntimeError(
                "Tabela compras_gov.raw_orgao não encontrada. "
                "Execute orgao_dag antes de contratos_item_dag."
            ) from exc
        orgaos = [str(row[0]) for row in rows]
        logging.info("Total de órgãos a processar: %s", len(orgaos))
        return orgaos

    @task(max_active_tis_per_dag=4)
    def ingest_orgao(codigo_orgao: str, **context: object) -> dict:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        itens = 0

        for batch, _ in api.iter_pages(
            "/modulo-contratos/2_consultarContratosItem",
            {
                "codigoOrgao": codigo_orgao,
                "dataVigenciaInicialMin": data_inicial,
                "dataVigenciaInicialMax": data_final,
            },
        ):
            db.insert_data(
                _stamp(batch),
                "raw_contratos_item",
                primary_key=["codigounidadegestora", "numerocontrato", "nifornecedor", "numeroitem", "contratoitemexcluido"],
                conflict_fields=["codigounidadegestora", "numerocontrato", "nifornecedor", "numeroitem", "contratoitemexcluido"],
                schema=SCHEMA,
            )
            itens += len(batch)

        logging.info("Órgão %s %s→%s: itens=%s", codigo_orgao, data_inicial, data_final, itens)
        return {"itens": itens}

    @task
    def validate(results: list[dict]) -> None:
        total_itens = sum(r["itens"] for r in results)
        logging.info("Contratos item total: itens=%s orgaos=%s", total_itens, len(results))

    orgaos = get_orgaos()
    results = ingest_orgao.expand(codigo_orgao=orgaos)
    validate(results)


contratos_item_dag()
