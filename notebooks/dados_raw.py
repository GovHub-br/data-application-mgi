"""CSVs locais das tabelas raw do banco, para os notebooks não baixarem tudo a cada execução.

Cada notebook lista as tabelas que usa e chama garantir_csvs. Os CSVs que já
estão em dados/raw/ são reaproveitados e só os que faltam são baixados. Uso:

    from dados_raw import garantir_csvs

    FORCAR_DOWNLOAD = False  # True baixa tudo de novo, com as últimas mudanças do banco
    garantir_csvs(["raw_orgao", "raw_uasg"], forcar=FORCAR_DOWNLOAD)

Depois, leia com read_csv_ids_str("dados/raw/raw_orgao.csv").
"""

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from db_config import testar_conexao

RAW_DIR = Path("dados/raw")
SCHEMA = "compras_gov"


def is_id_column(col: str) -> bool:
    """Colunas de identificação, que ficam como texto para não perder zeros à esquerda."""
    c = col.lower()
    return (
        "codigo" in c
        or "cnpj" in c
        or "cpf" in c
        or c.startswith(("id", "numero", "sequencial", "cod"))
        or c.endswith(("id", "idpncp"))
        or c in {"nifornecedor"}
    )


def exportar_tabela_csv(tabela: str, engine: Engine, pasta: Path = RAW_DIR, schema: str = SCHEMA) -> Path:
    """Salva schema.tabela em <pasta>/<tabela>.csv.

    Grava primeiro num arquivo temporário e só renomeia no fim: um download
    interrompido não deixa um CSV pela metade que pareceria completo.
    """
    df = pd.read_sql(f'SELECT * FROM "{schema}"."{tabela}"', engine)
    for col in df.columns:
        if is_id_column(col):
            df[col] = df[col].astype("string")

    pasta.mkdir(parents=True, exist_ok=True)
    destino = pasta / f"{tabela}.csv"
    temporario = pasta / f"{tabela}.parcial.csv"
    df.to_csv(temporario, index=False)
    os.replace(temporario, destino)
    linhas = f"{len(df):,}".replace(",", ".")
    print(f"Exportado: {tabela} → {destino} ({linhas} linhas)")
    return destino


def csvs_faltando(tabelas: list[str], pasta: Path = RAW_DIR) -> list[str]:
    """Tabelas cujo CSV não existe ou está vazio."""
    return [
        tabela for tabela in tabelas
        if not (arquivo := pasta / f"{tabela}.csv").is_file() or arquivo.stat().st_size == 0
    ]


def data_csv_mais_antigo(tabelas: list[str], pasta: Path = RAW_DIR) -> str:
    """Data e hora do download mais antigo entre os CSVs da pasta."""
    mais_antigo = min((pasta / f"{tabela}.csv").stat().st_mtime for tabela in tabelas)
    return datetime.fromtimestamp(mais_antigo).strftime("%d/%m/%Y %H:%M")


def garantir_csvs(tabelas: list[str], forcar: bool = False, pasta: Path = RAW_DIR, schema: str = SCHEMA) -> None:
    """Garante um CSV por tabela em pasta, baixando do banco só o necessário.

    Sem forcar, reaproveita os CSVs existentes e baixa os que faltam. Com
    forcar=True, baixa todos de novo. A conexão é testada sempre, para avisar
    que o banco está fora mesmo quando ele não é necessário.

    Levanta RuntimeError, antes de baixar qualquer tabela, quando precisa
    baixar e o banco não responde ou alguma tabela não existe nele.
    """
    faltando = csvs_faltando(tabelas, pasta)
    a_baixar = list(tabelas) if forcar else faltando
    engine = testar_conexao()

    if not a_baixar:
        print(f"Os {len(tabelas)} CSVs já estão em {pasta}/; seguindo sem baixar.")
    elif engine is not None:
        no_banco = set(inspect(engine).get_table_names(schema=schema))
        if ausentes := [tabela for tabela in a_baixar if tabela not in no_banco]:
            raise RuntimeError(
                f"Nada foi baixado: {len(ausentes)} tabela(s) não existem no schema "
                f"{schema} do banco: {', '.join(ausentes)}. "
                "Confira se as DAGs que carregam essas tabelas já rodaram."
            )
        print(f"Baixando {len(a_baixar)} tabela(s) para {pasta}/...")
        for tabela in a_baixar:
            exportar_tabela_csv(tabela, engine, pasta, schema)
    elif not faltando:  # só chega aqui com forcar=True
        raise RuntimeError(
            "FORCAR_DOWNLOAD = True, mas o banco não respondeu e nada foi baixado. "
            f"Os {len(tabelas)} CSVs históricos continuam em {pasta}/ "
            f"(o mais antigo foi baixado em {data_csv_mais_antigo(tabelas, pasta)}). "
            "Para usá-los, mude FORCAR_DOWNLOAD para False e rode a célula de novo."
        )
    else:
        raise RuntimeError(
            f"Faltam {len(faltando)} CSVs em {pasta}/ e o banco não respondeu: "
            + ", ".join(faltando)
        )
