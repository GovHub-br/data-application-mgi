from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
from mgi.cliente_compras_gov import ClienteComprasGov
from mgi.cliente_postgres import ClientPostgresDB
from mgi.helpers.postgres_helpers import get_postgres_conn

SCHEMA="compras_gov"

default_args = {
    "owner": "Zayra",
    "queue": "mgi",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def _stamp(records: list[dict]) -> list[dict]:
    ts = datetime.now().isoformat()
    for r in records:
        r["dt_ingest"] = ts
    return records

@dag(
    dag_id="pgc_detalhe_dag",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "pgc_detalhe", "raw"],
    max_active_tasks=4
)
def pgc_detalhe_dag() -> None:
    @task
    def get_codigos_orgao() -> list[str]:
        db = ClientPostgresDB(get_postgres_conn())

        rows = db.execute_query(
            f"""
            SELECT DISTINCT cnpjcpforgao FROM {SCHEMA}.raw_orgao WHERE cnpjcpforgao IS NOT NULL ORDER BY cnpjcpforgao
            """
        )
        if not rows:
            raise RuntimeError(
                "Tabela compras_gov.raw_orgao não encontrada ou vazia. "
                "Execute orgao_dag antes de pgc_detalhe_dag."
            )

        codigos = [str(r[0]) for r in rows]

        logging.info("PGC Detalhe: %s órgãos a processar", len(codigos))

        return codigos

    @task
    def fetch_pgc_detalhe(orgao: str) -> dict:
        api= ClienteComprasGov()
        db= ClientPostgresDB(get_postgres_conn())

        resultados={}
        for ano in range(2023,2027):
            try:
                pgc, metadata = api.fetch_all_pages(
                    "/modulo-pgc/1_consultarPgcDetalhe",
                    {"orgao": orgao, "anoPcaProjetoCompra": ano}
                )
                logging.info("PGC Detalhe: órgão=%s, ano=%s, registros=%s", orgao, ano, len(pgc))
                if pgc:
                    logging.info("Inserindo %s registros de PGC Detalhe no banco de dados", len(pgc))
                    records = _stamp(pgc)
                    db.insert_data(records, "raw_pgc_detalhe",schema=SCHEMA)
                    resultados[ano] = len(pgc)
                else:
                    logging.info("Sem registros: órgão=%s, ano=%s", orgao, ano)
                    resultados[ano] = 0

            except Exception as exc:
                logging.error("Erro ao buscar PGC Detalhe para órgão=%s, ano=%s: %s", orgao, ano, exc)


        return {"orgao": orgao, "ano": resultados}

            

    codigos = get_codigos_orgao()
    resultados = fetch_pgc_detalhe.expand(orgao=codigos)

pgc_detalhe_dag()