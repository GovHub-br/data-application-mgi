import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from mgi.cliente_compras_gov import ClienteComprasGov
from mgi.cliente_postgres import ClientPostgresDB
from mgi.helpers.postgres_helpers import get_postgres_conn

SCHEMA = "compras_gov"
BLOCK_SIZE = 150

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
    dag_id="arp_detalhes_dag",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "arp", "raw"],
)
def arp_detalhes_dag() -> None:
    @task
    def get_item_offsets() -> list[int]:
        db = ClientPostgresDB(get_postgres_conn())
        rows = db.execute_query(f"""
            SELECT COUNT(DISTINCT (numeroataregistropreco, codigounidadegerenciadora, numeroitem))
            FROM {SCHEMA}.raw_arp_item
        """)
        total = rows[0][0]
        offsets = list(range(0, total, BLOCK_SIZE))
        logging.info("ARP detalhes itens: %s itens em %s blocos", total, len(offsets))
        return offsets

    @task
    def get_par_offsets() -> list[int]:
        db = ClientPostgresDB(get_postgres_conn())
        rows = db.execute_query(f"""
            SELECT COUNT(DISTINCT (numeroataregistropreco, codigounidadegerenciadora))
            FROM {SCHEMA}.raw_arp_item
        """)
        total = rows[0][0]
        offsets = list(range(0, total, BLOCK_SIZE))
        logging.info("ARP detalhes empenhos: %s atas em %s blocos", total, len(offsets))
        return offsets

    @task(max_active_tis_per_dag=4)
    def fetch_unidades_adesoes(offset: int) -> dict:
        db = ClientPostgresDB(get_postgres_conn())
        rows = db.execute_query(f"""
            SELECT DISTINCT
                numeroataregistropreco,
                codigounidadegerenciadora,
                numeroitem
            FROM {SCHEMA}.raw_arp_item
            ORDER BY numeroataregistropreco, codigounidadegerenciadora, numeroitem
            LIMIT {BLOCK_SIZE} OFFSET {offset}
        """)
        api = ClienteComprasGov()
        total_unidades = 0
        total_adesoes = 0
        for ata, ug, item in rows:
            ctx = {"numeroataregistropreco": str(ata), "codigounidadegerenciadora": str(ug), "numeroitem": str(item)}
            unidades, _ = api.consultar_arp_unidades_item(str(ata), str(ug), str(item))
            if unidades:
                db.insert_data(_stamp([{**ctx, **r} for r in unidades]), "raw_arp_unidades_item", schema=SCHEMA)
            adesoes, _ = api.consultar_arp_adesoes_item(str(ata), str(ug), str(item))
            if adesoes:
                db.insert_data(_stamp([{**ctx, **r} for r in adesoes]), "raw_arp_adesoes_item", schema=SCHEMA)
            total_unidades += len(unidades)
            total_adesoes += len(adesoes)
        return {"unidades": total_unidades, "adesoes": total_adesoes}

    @task(max_active_tis_per_dag=4)
    def fetch_empenhos(offset: int) -> dict:
        db = ClientPostgresDB(get_postgres_conn())
        rows = db.execute_query(f"""
            SELECT DISTINCT
                numeroataregistropreco,
                codigounidadegerenciadora
            FROM {SCHEMA}.raw_arp_item
            ORDER BY numeroataregistropreco, codigounidadegerenciadora
            LIMIT {BLOCK_SIZE} OFFSET {offset}
        """)
        api = ClienteComprasGov()
        total_empenhos = 0
        for ata, ug in rows:
            ctx = {"numeroataregistropreco": str(ata), "codigounidadegerenciadora": str(ug)}
            empenhos, _ = api.consultar_arp_empenhos_saldo(str(ata), str(ug))
            if empenhos:
                db.insert_data(_stamp([{**ctx, **r} for r in empenhos]), "raw_arp_empenhos_saldo", schema=SCHEMA)
            total_empenhos += len(empenhos)
        return {"empenhos": total_empenhos}

    @task
    def validate_itens(results: list[dict]) -> None:
        total_unidades = sum(r["unidades"] for r in results)
        total_adesoes = sum(r["adesoes"] for r in results)
        logging.info("ARP detalhes itens: unidades=%s adesoes=%s", total_unidades, total_adesoes)

    @task
    def validate_empenhos(results: list[dict]) -> None:
        total = sum(r["empenhos"] for r in results)
        logging.info("ARP detalhes empenhos: total=%s", total)

    item_offsets = get_item_offsets()
    par_offsets = get_par_offsets()
    validate_itens(fetch_unidades_adesoes.expand(offset=item_offsets))
    validate_empenhos(fetch_empenhos.expand(offset=par_offsets))


arp_detalhes_dag()
