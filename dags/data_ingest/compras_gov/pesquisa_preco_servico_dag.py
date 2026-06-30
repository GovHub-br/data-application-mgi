import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from cliente_compras_gov import ClienteComprasGov
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

SCHEMA = "compras_gov"

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


@dag(
    dag_id="pesquisa_preco_servico_dag",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tis_per_dag=4,
    default_args=default_args,
    tags=["mgi", "compras_gov", "pesquisa_preco", "raw"],
)
def pesquisa_preco_servico_dag() -> None:
    @task
    def get_itens_servico() -> list[str]:
        db = ClientPostgresDB(get_postgres_conn())
        rows = db.execute_query(
            f"SELECT DISTINCT codigoservico FROM {SCHEMA}.raw_item_servico ORDER BY codigoservico"
        )
        itens = [str(r[0]) for r in rows]
        logging.info("Pesquisa de preços serviço: %s itens a processar", len(itens))
        return itens

    @task
    def fetch_preco_servico(codigo_item: str) -> dict:
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())

        preco, _ = api.fetch_all_pages(
            "/modulo-pesquisa-preco/3_consultarServico",
            {"codigoItemCatalogo": codigo_item},
        )
        if preco:
            db.insert_data(_stamp(preco), "raw_pesquisa_preco_servico", schema=SCHEMA)

        detalhe, _ = api.fetch_all_pages(
            "/modulo-pesquisa-preco/4_consultarServicoDetalhe",
            {"codigoItemCatalogo": codigo_item},
        )
        if detalhe:
            db.insert_data(_stamp(detalhe), "raw_pesquisa_preco_servico_detalhe", schema=SCHEMA)

        return {"preco": len(preco), "detalhe": len(detalhe)}

    @task
    def validate(results: list[dict]) -> None:
        total_preco = sum(r["preco"] for r in results)
        total_detalhe = sum(r["detalhe"] for r in results)
        logging.info(
            "Pesquisa preço serviço: itens=%s preco=%s detalhe=%s",
            len(results), total_preco, total_detalhe,
        )

    itens = get_itens_servico()
    results = fetch_preco_servico.expand(codigo_item=itens)
    validate(results)


pesquisa_preco_servico_dag()
