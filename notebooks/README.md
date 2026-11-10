# Notebooks de análise exploratória

Notebooks Jupyter para explorar os dados do ComprasGov (materiais, serviços,
órgãos/UASGs, contratações, contratos e ARP). Eles rodam num ambiente conda
próprio, separado do Airflow, e leem as credenciais do PostgreSQL de um arquivo
local `.env` que nunca vai para o git.

Todos os comandos abaixo partem da **raiz do repositório**.

## 1. Ambiente conda

Requer o [Miniforge](https://github.com/conda-forge/miniforge) (conda com o
canal `conda-forge`).

```bash
conda env create -f notebooks/environment.yml
conda activate mgi-notebooks
```

O ambiente usa Python 3.11, a mesma versão do projeto. Para atualizar depois de
uma mudança no `environment.yml`:

```bash
conda env update -f notebooks/environment.yml --prune
```

Registre o ambiente como kernel do Jupyter (uma vez por máquina):

```bash
python -m ipykernel install --user --name mgi-notebooks --display-name "Python 3.11 (conda: mgi-notebooks)"
```

Abra os notebooks e escolha o kernel **Python 3.11 (conda: mgi-notebooks)**:

```bash
jupyter lab notebooks/
```

No VS Code, abra o `.ipynb` e selecione o mesmo kernel.

## 2. Credenciais do banco

As credenciais ficam em `notebooks/.env`. O arquivo é ignorado pelo git e
existe só na sua máquina. **Nunca** escreva usuário ou senha numa célula de
notebook: a célula é salva dentro do `.ipynb` e iria para o histórico do git.

Crie o arquivo a partir do modelo, deixando a leitura restrita ao seu usuário:

```bash
cp notebooks/.env.example notebooks/.env
chmod 600 notebooks/.env
```

Preencha os valores num editor, por exemplo `nano notebooks/.env`. No nano,
**Ctrl+O** e **Enter** salvam, e **Ctrl+X** sai. Escreva cada valor logo
depois do `=`:

| Variável | Valor |
|---|---|
| `DB_USER` | usuário do banco |
| `DB_PASSWORD` | senha do usuário |
| `DB_HOST` | endereço do servidor |
| `DB_PORT` | porta (o padrão do PostgreSQL é `5432`) |
| `DB_NAME` | nome do banco |

Regras de preenchimento:

- Não use espaços ao redor do `=`.
- Não use aspas, nem para senhas com `@`, `:` ou `/`.
- Se a senha tiver `#`, espaço ou aspas, coloque o valor entre aspas simples:
  `DB_PASSWORD='abc #123'`.
- Não grave a senha com `echo` no terminal, porque ela fica no histórico do
  shell.

Para conferir sem mostrar os valores, este comando lista as variáveis vazias:

```bash
grep -E '^DB_[A-Z]+=$' notebooks/.env || echo "todas preenchidas"
```

## 3. Conectar no notebook

O módulo [`db_config.py`](db_config.py) lê o `.env` e cria o engine do
SQLAlchemy:

```python
from db_config import get_engine

engine = get_engine()
```

O `get_engine()` monta a URL com `URL.create`, que escapa caracteres especiais
da senha. Ao exibir o engine, a senha aparece como `***`. Se o notebook já tem
uma função própria de conexão que recebe `user`, `password`, `host`, `port` e
`database`, use `load_db_credentials()`:

```python
from db_config import load_db_credentials

engine = get_postgres_engine(**load_db_credentials())
```

O import funciona porque o Jupyter executa o notebook a partir da pasta
`notebooks/`.

Para testar a conexão:

```python
with engine.connect() as conn:
    print(conn.exec_driver_sql("select current_user, current_database()").one())
```

### Tabelas em CSV

Os notebooks não fazem a análise direto no banco: eles leem um CSV por tabela
do schema `compras_gov`, em `notebooks/dados/raw/`. A segunda célula de código
de cada notebook lista as tabelas que ele usa e chama `garantir_csvs`, de
[`dados_raw.py`](dados_raw.py):

```python
from dados_raw import garantir_csvs

FORCAR_DOWNLOAD = False
TABELAS = ["raw_orgao", "raw_uasg"]

garantir_csvs(TABELAS, forcar=FORCAR_DOWNLOAD)
```

- Os CSVs que já estão na pasta são reaproveitados; só os que faltam são
  baixados.
- Para baixar tudo de novo, com as últimas mudanças do banco, mude
  `FORCAR_DOWNLOAD` para `True`. Depois volte para `False`, senão todo run
  baixa tudo.
- A conexão é testada sempre, com timeout de 5 s. A mensagem de erro troca
  usuário, senha e host por `***`.
- Se a célula precisa baixar e o banco não responde, ela para com erro. Com
  `FORCAR_DOWNLOAD = True` e todos os CSVs na pasta, o erro informa a data do
  download mais antigo e orienta voltar para `False` para usá-los.

Os CSVs são ignorados pelo git.

## 4. Erros comuns

| Mensagem | Causa |
|---|---|
| `Variáveis ausentes em .../notebooks/.env: ...` | O `.env` não existe ou tem linha vazia. |
| `ModuleNotFoundError: No module named 'db_config'` | O notebook não está rodando a partir de `notebooks/`. |
| `ModuleNotFoundError: No module named 'dotenv'` | O kernel selecionado não é o `mgi-notebooks`. |
| `password authentication failed` | Usuário ou senha incorretos no `.env`. |
| `could not translate host name` ou timeout | Host incorreto, ou o banco exige VPN. |

## 5. Proteção contra vazamento de senhas

O repositório usa o [betterleaks](https://github.com/betterleaks/betterleaks)
para bloquear commits que contenham senhas, tokens ou chaves. O
[`.betterleaks.toml`](../.betterleaks.toml) usa as regras padrão e acrescenta
duas regras que elas não cobrem: senha embutida numa URL de conexão
`postgresql://` e senha digitada numa célula de `.ipynb`.

Instale o betterleaks (binário em
[releases](https://github.com/betterleaks/betterleaks/releases)) e ative o hook
de pre-commit, que vale para todos os worktrees do repositório:

```bash
install -m 755 scripts/git-hooks/pre-commit-betterleaks \
  "$(git rev-parse --path-format=absolute --git-common-dir)/hooks/pre-commit"
```

Com o hook ativo, o commit é bloqueado se o betterleaks encontrar uma senha. A
saída mostra a linha, mas não a senha. Se for um falso positivo (por exemplo,
o nome de uma variável), adicione o comentário `betterleaks:allow` na linha.

Para verificar uma pasta manualmente:

```bash
betterleaks dir notebooks/ -c .betterleaks.toml --redact
```

Se uma senha chegou a ser commitada, removê-la do arquivo não basta, porque ela
continua no histórico do git. Troque a senha no banco e avise o time.
