"""Credenciais do PostgreSQL para os notebooks, lidas de notebooks/.env.

Mantém usuário e senha fora do código e das saídas dos notebooks. Uso:

    from db_config import get_engine
    engine = get_engine()

Para testar a conexão sem travar o notebook quando o banco está fora:

    from db_config import testar_conexao
    engine = testar_conexao()  # None se o banco não responder

Ou, com uma função própria que recebe user, password, host, port e database:

    from db_config import load_db_credentials
    engine = get_postgres_engine(**load_db_credentials())
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

ENV_PATH = Path(__file__).resolve().parent / ".env"

_ENV_VARS = {
    "user": "DB_USER",
    "password": "DB_PASSWORD",  # betterleaks:allow (nome da variável, não a senha)
    "host": "DB_HOST",
    "port": "DB_PORT",
    "database": "DB_NAME",
}


def load_db_credentials(env_path: Path = ENV_PATH) -> dict[str, str]:
    """Lê as credenciais do .env; as chaves batem com get_postgres_engine."""
    load_dotenv(env_path)
    credentials = {key: os.getenv(var, "") for key, var in _ENV_VARS.items()}
    missing = [_ENV_VARS[key] for key, value in credentials.items() if not value]
    if missing:
        raise RuntimeError(
            f"Variáveis ausentes em {env_path}: {', '.join(missing)}. "
            "Copie notebooks/.env.example para notebooks/.env e preencha."
        )
    return credentials


def get_engine(env_path: Path = ENV_PATH, **engine_kwargs) -> Engine:
    """Cria o engine do SQLAlchemy sem expor a senha.

    URL.create escapa caracteres especiais da senha (@, :, /), que quebrariam
    uma URL montada com f-string. engine_kwargs vai direto para create_engine,
    por exemplo connect_args={"connect_timeout": 5}.
    """
    credentials = load_db_credentials(env_path)
    url = URL.create(
        "postgresql+psycopg2",
        username=credentials["user"],
        password=credentials["password"],
        host=credentials["host"],
        port=int(credentials["port"]),
        database=credentials["database"],
    )
    return create_engine(url, **engine_kwargs)


def ocultar_credenciais(mensagem: str, env_path: Path = ENV_PATH) -> str:
    """Troca usuário, senha e host por *** para não gravá-los na saída do .ipynb."""
    credentials = load_db_credentials(env_path)
    for key in ("password", "user", "host"):
        mensagem = mensagem.replace(credentials[key], "***")
    return mensagem


def testar_conexao(timeout: int = 5, env_path: Path = ENV_PATH) -> Engine | None:
    """Devolve o engine se o banco responder; senão avisa o motivo e devolve None.

    O timeout evita esperar o timeout do TCP (cerca de 2 minutos) quando a VPN
    está desligada.
    """
    try:
        engine = get_engine(env_path, connect_args={"connect_timeout": timeout})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except RuntimeError as erro:  # .env ausente ou com variável vazia
        print(f"Conexão com o banco FALHOU: {erro}")
        return None
    except SQLAlchemyError as erro:
        motivo = str(getattr(erro, "orig", erro)).strip().splitlines()[0]
        print(f"Conexão com o banco FALHOU: {ocultar_credenciais(motivo, env_path)}")
        return None
    print("Conexão com o banco OK.")
    return engine
