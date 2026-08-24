from datetime import datetime, timedelta
import logging
from mgi.cliente_compras_gov import ClienteComprasGov
from mgi.cliente_postgres import ClientPostgresDB
from mgi.helpers.postgres_helpers import get_postgres_conn
from airflow.sdk import dag, task, Param, get_current_context

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
    params={
        "modo": Param(
            "incremental",enum=["incremental", "fria"], description="Modo de execução do DAG: incremental ou full"),
        "ano_inicio": Param(2023, type="integer", description="Ano inicial para consulta de PGC Detalhe (2023)"),
        "ano_fim": Param(2025, type="integer", description="Ano final para consulta de PGC Detalhe (ano atual-1)"),
    },
    max_active_tasks=4
)
def pgc_detalhe_dag() -> None:

    @task
    def get_codigos_orgao() -> list[str]:
        db = ClientPostgresDB(get_postgres_conn())

        rows = db.execute_query(
            f"""
            SELECT DISTINCT cnpjcpforgao FROM {SCHEMA}.raw_orgao WHERE cnpjcpforgao IS NOT NULL ORDER BY cnpjcpforgao limit 5
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
        context = get_current_context()
        modo=context["params"]["modo"]
        if modo == "fria":
            ano_inicio=context["params"]["ano_inicio"]
            ano_fim=context["params"]["ano_fim"]
            anos=list(range(ano_inicio, ano_fim+1))
            logging.info("Modo de execução: fria. Anos a processar: %s", anos)
        else:
            current_date=datetime.now()
            ano_atual=current_date.year
            anos=[ano_atual]
            logging.info("Modo de execução: incremental. Ano a processar: %s", anos)

        api= ClienteComprasGov()
        db= ClientPostgresDB(get_postgres_conn())

        resultados={}
        for ano in anos:
            try:
                pgc, metadata = api.fetch_all_pages(
                    "/modulo-pgc/1_consultarPgcDetalhe",
                    {"orgao": orgao, "anoPcaProjetoCompra": ano}
                )
                logging.info("PGC Detalhe: órgão=%s, ano=%s, registros=%s", orgao, ano, len(pgc))

                if modo == "incremental":
                    delete_query=f"""
                        DELETE FROM {SCHEMA}.raw_pgc_detalhe WHERE anoPcaProjetoCompra='{ano}'
                        AND cnpjcpforgao='{orgao}'
                    """
                    db.execute_non_query(delete_query)
                    logging.info("Registros de PGC Detalhe para o ano %s e órgão %s excluídos da tabela raw_pgc_detalhe", ano, orgao)

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
                raise


        return {"orgao": orgao, "ano": resultados}

    @task
    def validate(results: list[dict]) -> None:
        total_registros = sum(sum(ano.values()) for ano in (r["ano"] for r in results))
        logging.info(
            "PGC Detalhe: órgãos=%s, total registros=%s",
            len(results), total_registros,
        )

    codigos_orgao=get_codigos_orgao()
    resultados=fetch_pgc_detalhe.expand(orgao=codigos_orgao)
    validate(resultados)


pgc_detalhe_dag()

