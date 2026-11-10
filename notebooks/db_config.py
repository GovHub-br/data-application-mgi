"""Credenciais do PostgreSQL para os notebooks, lidas de notebooks/.env.

Mantém usuário e senha fora do código e das saídas dos notebooks. Uso:

    from db_config import get_engine
    engine = get_engine()

Ou, com a função get_postgres_engine do notebook 2:

    from db_config import load_db_credentials
    engine = get_postgres_engine(**load_db_credentials())
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine
from sqlalchemy.engine import Engine

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
