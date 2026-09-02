from datetime import datetime, timedelta
import logging

from airflow.sdk import dag, task, Param, get_current_context
from mgi.cliente_compras_gov import ClienteComprasGov
from mgi.cliente_postgres import ClientPostgresDB
from mgi.helpers.postgres_helpers import get_postgres_conn

SCHEMA="compras_gov"

default_args = {
    "owner": "Agatha",
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
    dag_id="pgc_agregacao_dag",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "pgc_detalhe", "raw"],
    params={
        "modo": Param("incremental", enum=["incremental", "fria"], description="Modo de execução do DAG: incremental ou fria"), 
        "ano_inicio": Param(2023, type="integer", description="Ano de início para a execução do dag PGC Agregação, (2023)"),
    },
    max_active_tasks=4
)   

def pgc_agregacao_dag() -> None:
    @task 
    def delete_raw_current_year() -> None:
        context = get_current_context()
        modo = context["params"]["modo"]
        if modo == "fria":
            logging.info("Modo de execução: fria. Nenhum resgistro será excluído da tabela raw_pgc_agregacao")

            return 
        logging.info("Modo de execução: incremental. Excluindo registros do ano corrente da tabela raw_pgc_agregacao")
        db = ClientPostgresDB(get_postgres_conn())
        current_year = datetime.now().year
        ano_atual= current_year
        delete_query = f"""
            DELETE FROM {SCHEMA}.raw_pgc_agregacao WHERE ano = '{ano_atual}'
        """
        db.execute_non_query(delete_query)
        logging.info("Registros do ano corrente (%s) excluídos da tabela raw_pgc_agregacao", ano_atual)

    @task
    def get_codigos_pgc() -> list[str]:
        db = ClientPostgresDB(get_postgres_conn())

        rows = db.execute_query(
            f"SELECT DISTINCT cnpjcpforgaovinculado FROM {SCHEMA}.raw_orgao WHERE cnpjcpforgaovinculado IS NOT NULL ORDER BY cnpjcpforgaovinculado")
      
        if not rows:
            raise RuntimeError(
                "Tabela compras_gov.raw_orgao não encontrada ou vazia",
                "Execute o orgao_dag antes de executar este pgc_agregacao_dag"
            )

        codigos = [str(r[0]) for r in rows]
        logging.info("PGC Agregação: %s órgãos a processar", len(codigos))
        return codigos 

    @task(max_active_tis_per_dag=2)
    def fetch_agregacao_pgc(codigo_orgao: str) -> dict:
        context = get_current_context() 
        current_year = datetime.now().year
        ano_atual = current_year
        modo = context["params"]["modo"]
        if modo == "fria":
            ano_inicio = context["params"]["ano_inicio"]
            ano_fim = ano_atual
            anos = range(ano_inicio, ano_fim + 1)
            logging.info("Modo de execução: fria. Anos a procesar: %s", list(anos))
        else:
            anos = [ano_atual]
            logging.info("Modo de execução: incremental. Ano a procesar: %s", anos)

        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())

        resultados={}
        for ano in anos:
            try: 
                pgc, metadata = api.fetch_all_pages(
                    "/modulo-pgc/3_consultarPgcAgregacao",
                    {"orgao": codigo_orgao, "ano": ano}

                )
                if pgc: 
                    logging.info("PGC Agregação: %s registros encontrados para o orgão %s no ano %s", len(pgc), codigo_orgao, ano) 
                    records = _stamp(pgc) 
                    db.insert_data(records, "raw_pgc_agregacao", schema=SCHEMA) 
                    resultados[ano] = len(pgc) 
                else:
                    logging.info("PGC Agregação: Nenhum registro encontrado para o orgão %s no ano %s", codigo_orgao, ano)
                    resultados[ano] = 0

            except Exception as exc: 
                    logging.error("PGC Agregação: Erro ao buscar dados para o orgão %s no ano %s: %s", codigo_orgao, ano, str(exc))

        return {"codigo_orgao": codigo_orgao, "resultados": resultados}

    @task 
    def validate (results: list[dict]) -> None: 
        total_registros = sum(sum(r["resultados"].values()) for r in results) 

        logging.info(
            "PGC Agregação: total de registros inseridos: %s, %s",len(results), total_registros
        )

    codigos = get_codigos_pgc()
    delete_raw_current_year()
    results = fetch_agregacao_pgc.expand(codigo_orgao=codigos)
    validate(results)

pgc_agregacao_dag()

