from datetime import datetime, timedelta
import logging
from airflow.sdk import dag, task

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
)
def pgc_detalhe_catalogo_dag() -> None:
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
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        codigo_item = item["codigo"]
        tipo = item["tipo"]
        resultados = {}

        for ano in range(2023, 2027):
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
 
    itens = get_codigos_itens()
    resultados = fetch_pgc_detalhe_catalogo.expand(item=itens)
    validate_resultados(resultados)

pgc_detalhe_catalogo_dag()