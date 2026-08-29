from datetime import datetime, timedelta
import logging
from airflow.sdk import dag, task, Param, get_current_context

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
    dag_id="pgc_detalhe_catalogo_dag",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "pgc_detalhe_catalogo", "raw"],
    params={
        "modo": Param("incremental", enum=["incremental", "fria"], description="Modo de execução da DAG: incremental ou full"),
        "ano_inicio": Param(2023, type="integer", description="Ano inicial para consulta do PGC Detalhe Catalogo (2023)"),
    },
)
def pgc_detalhe_catalogo_dag() -> None:
    @task
    def delete_raw_current_year() -> None:
        context = get_current_context()
        modo=context["params"]["modo"]
        if modo == "fria":
            logging.info("Modo de execução: fria. Nenhum registro será excluído da tabela raw_pgc_detalhe_catalogo")
            return
        logging.info("Modo de execução: incremental. Excluindo registros de PGC Detalhe Catalogo do ano atual da tabela raw_pgc_detalhe_catalogo")
        current_date=datetime.now()
        ano_atual=current_date.year
        db= ClientPostgresDB(get_postgres_conn())
        delete_query=f"DELETE FROM {SCHEMA}.raw_pgc_detalhe_catalogo WHERE anoPcaProjetoCompra='{ano_atual}'"
        db.execute_non_query(delete_query)
        logging.info("Registros de PGC Detalhe Catalogo para o ano %s excluídos da tabela raw_pgc_detalhe_catalogo", ano_atual)

    @task
    def get_codigos_itens() -> list[dict]:
        db = ClientPostgresDB(get_postgres_conn())
        rows_material = db.execute_query(
            f"SELECT DISTINCT codigoclasse FROM {SCHEMA}.raw_item_material ORDER BY codigoclasse limit 50"
        )
        rows_servico = db.execute_query(
            f"SELECT DISTINCT codigogrupo FROM {SCHEMA}.raw_item_servico ORDER BY codigogrupo limit 50"
        )
        itens = [{"codigo": str(r[0]), "tipo": "Material"} for r in rows_material]
        itens += [{"codigo": str(r[0]), "tipo": "Servico"} for r in rows_servico]
        logging.info("PGC Detalhe Catalogo: %s itens a processar", len(itens))
        return itens

    @task(max_active_tis_per_dag=4)
    def fetch_pgc_detalhe_catalogo(item : dict) -> dict:
        context = get_current_context()
        current_date=datetime.now()
        ano_atual=current_date.year
        modo=context["params"]["modo"]

        if modo == "fria":
            ano_inicio=context["params"]["ano_inicio"]
            ano_fim=ano_atual
            anos=list(range(ano_inicio, ano_fim+1))
            logging.info("Modo de execução: fria. Anos a processar: %s", anos)
        else:
            anos=[ano_atual]
            logging.info("Modo de execução: incremental. Ano a processar: %s", anos)

        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        codigo_item = item["codigo"]
        tipo = item["tipo"]
        resultados = {}

        for ano in anos:
            try:
                pgc, metadata = api.fetch_all_pages(
                    "/modulo-pgc/2_consultarPgcDetalheCatalogo",
                    {"anoPcaProjetoCompra": ano, "tipo": tipo, "codigo": codigo_item},
                )

                if pgc:
                    records = _stamp(pgc)
                    db.insert_data(records, "raw_pgc_detalhe_catalogo", schema=SCHEMA)
                    resultados[ano] = len(pgc)
                    logging.info(
                        "PGC Detalhe Catalogo: ano=%s, tipo=%s, codigo=%s, registros=%s",
                        ano, tipo, codigo_item, len(pgc),
                    )
                else:
                    resultados[ano] = 0
                    logging.info(
                        "Sem registros: ano=%s, tipo=%s, codigo=%s", ano, tipo, codigo_item
                    )

            except Exception:
                resultados[ano] = None
                logging.exception(
                    "Erro ao buscar PGC Detalhe Catalogo para ano=%s, tipo=%s, codigo=%s",
                    ano, tipo, codigo_item,
                )

        return {"codigo": codigo_item, "tipo": tipo, "ano": resultados}
    
    @task
    def validate_resultados(resultados_itens : list[dict]) -> None:
        total_itens = len(resultados_itens)
        total_registros = sum(sum(r["ano"].values()) for r in resultados_itens)
        logging.info("Validação: %s itens, %s registros", total_itens, total_registros)

    delete_current_year=delete_raw_current_year()
    itens = get_codigos_itens()
    resultados = fetch_pgc_detalhe_catalogo.expand(item=itens)

    delete_current_year >> resultados 
    itens >> resultados

    validate_resultados(resultados)

pgc_detalhe_catalogo_dag()