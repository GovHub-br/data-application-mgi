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
    "retry_delay": timedelta(minutes=5),
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
    dag_id="arp_detalhes_dag",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["mgi", "compras_gov", "arp", "raw"],
)
def arp_detalhes_dag() -> None:
    @task
    def get_items(**context: object) -> list[dict]:
        """Retorna itens de raw_arp_item filtrados pelo intervalo de datas."""
        data_inicial, data_final = _get_intervalo(context)
        db = ClientPostgresDB(get_postgres_conn())
        rows = db.execute_query(f"""
            SELECT DISTINCT
                numeroataregistropreco,
                codigounidadegerenciadora,
                numeroitem
            FROM {SCHEMA}.raw_arp_item
            WHERE datavigenciainicial BETWEEN '{data_inicial}' AND '{data_final}'
            ORDER BY numeroataregistropreco, codigounidadegerenciadora, numeroitem
        """)
        items = [{"ata": str(r[0]), "ug": str(r[1]), "item": str(r[2])} for r in rows]
        logging.info("ARP detalhes: %s itens para processar (%s→%s)", len(items), data_inicial, data_final)
        return items

    @task
    def get_pares_ata(**context: object) -> list[dict]:
        """Retorna pares únicos (ata, ug) de raw_arp_item para busca de empenhos."""
        data_inicial, data_final = _get_intervalo(context)
        db = ClientPostgresDB(get_postgres_conn())
        rows = db.execute_query(f"""
            SELECT DISTINCT
                numeroataregistropreco,
                codigounidadegerenciadora
            FROM {SCHEMA}.raw_arp_item
            WHERE datavigenciainicial BETWEEN '{data_inicial}' AND '{data_final}'
            ORDER BY numeroataregistropreco, codigounidadegerenciadora
        """)
        pares = [{"ata": str(r[0]), "ug": str(r[1])} for r in rows]
        logging.info("ARP detalhes: %s atas para empenhos (%s→%s)", len(pares), data_inicial, data_final)
        return pares

    @task(max_active_tis_per_dag=4)
    def fetch_unidades_adesoes(args: dict) -> dict:
        """Busca unidades (endpoint 3) e adesões (endpoint 5) para um item."""
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        ata, ug, item = args["ata"], args["ug"], args["item"]

        unidades, _ = api.consultar_arp_unidades_item(ata, ug, item)
        if unidades:
            db.insert_data(_stamp(unidades), "raw_arp_unidades_item", schema=SCHEMA)

        adesoes, _ = api.consultar_arp_adesoes_item(ata, ug, item)
        if adesoes:
            db.insert_data(_stamp(adesoes), "raw_arp_adesoes_item", schema=SCHEMA)

        logging.info("Item ata=%s ug=%s item=%s: unidades=%s adesoes=%s", ata, ug, item, len(unidades), len(adesoes))
        return {"unidades": len(unidades), "adesoes": len(adesoes)}

    @task(max_active_tis_per_dag=4)
    def fetch_empenhos(args: dict) -> dict:
        """Busca empenhos e saldos (endpoint 4) para uma ata."""
        api = ClienteComprasGov()
        db = ClientPostgresDB(get_postgres_conn())
        ata, ug = args["ata"], args["ug"]

        empenhos, _ = api.consultar_arp_empenhos_saldo(ata, ug)
        if empenhos:
            db.insert_data(_stamp(empenhos), "raw_arp_empenhos_saldo", schema=SCHEMA)

        logging.info("Ata ata=%s ug=%s: empenhos=%s", ata, ug, len(empenhos))
        return {"empenhos": len(empenhos)}

    @task
    def validate(results_items: list[dict], results_atas: list[dict]) -> None:
        total_unidades = sum(r["unidades"] for r in results_items)
        total_adesoes = sum(r["adesoes"] for r in results_items)
        total_empenhos = sum(r["empenhos"] for r in results_atas)
        logging.info(
            "ARP detalhes: unidades=%s adesoes=%s empenhos=%s",
            total_unidades, total_adesoes, total_empenhos,
        )

    items = get_items()
    pares = get_pares_ata()
    results_items = fetch_unidades_adesoes.expand(args=items)
    results_atas = fetch_empenhos.expand(args=pares)
    validate(results_items=results_items, results_atas=results_atas)


arp_detalhes_dag()
