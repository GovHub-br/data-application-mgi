import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from mgi.cliente_compras_gov import ClienteComprasGov
from mgi.cliente_postgres import ClientPostgresDB
from mgi.helpers.postgres_helpers import get_postgres_conn
from airflow.providers.postgres.hooks.postgres import PostgresHook

SCHEMA = "compras_gov"
PAGE_SIZE = 500

default_args = {
    "owner": "mgi",
    "queue": "mgi",
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
    dag_id="contratos_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "contratos", "raw"],
)
def contratos_dag() -> None:
    @task
    def get_orgaos() -> list[str]:
        db = ClientPostgresDB(get_postgres_conn())
        try:
            rows = db.execute_query(
                f"SELECT DISTINCT codigoorgao FROM {SCHEMA}.raw_orgao "
                "ORDER BY codigoorgao"
            )
        except Exception as exc:
            raise RuntimeError(
                "Tabela compras_gov.raw_orgao não encontrada. "
                "Execute orgao_dag antes de contratos_dag."
            ) from exc
        orgaos = [str(row[0]) for row in rows]
        logging.info("Total de órgãos a processar: %s", len(orgaos))
        return orgaos

    @task(max_active_tis_per_dag=4)
    def ingest_orgao(codigo_orgao: str, **context: object) -> dict:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        contratos = 0

        for batch, _ in api.iter_pages(
            "/modulo-contratos/1_consultarContratos",
            {
                "codigoOrgao": codigo_orgao,
                "dataVigenciaInicialMin": data_inicial,
                "dataVigenciaInicialMax": data_final,
            },
        ):
            db.insert_data(
                _stamp(batch),
                "raw_contratos",
                primary_key=["codigounidadegestora", "numerocontrato", "nifornecedor"],
                conflict_fields=[
                    "codigounidadegestora",
                    "numerocontrato",
                    "nifornecedor",
                ],
                schema=SCHEMA,
            )
            contratos += len(batch)

        logging.info(
            "Órgão %s %s→%s: contratos=%s",
            codigo_orgao,
            data_inicial,
            data_final,
            contratos,
        )
        return {"contratos": contratos}

    @task
    def validate() -> None:
        pg_hook = PostgresHook()
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.raw_contratos")
                total_contratos = cursor.fetchone()[0]
                logging.info("Total de contratos na tabela: %s", total_contratos)

    orgaos = get_orgaos()
    results = ingest_orgao.expand(codigo_orgao=orgaos)
    results >> validate()


contratos_dag()
