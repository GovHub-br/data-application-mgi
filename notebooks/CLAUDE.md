# notebooks/: instruções para o Claude

Guia completo (para humanos): @README.md

## Credenciais

- NUNCA leia, exiba, copie ou edite `notebooks/.env`: ele tem credenciais reais.
  Para saber quais variáveis existem, leia `notebooks/.env.example`.
- NUNCA escreva usuário, senha ou URL de conexão com senha em células, código,
  saídas ou mensagens de commit. Conecte sempre com
  `from db_config import get_engine; engine = get_engine()`.
- Para checar se o `.env` está preenchido, use só
  `grep -E '^DB_[A-Z]+=$' notebooks/.env || echo "todas preenchidas"`,
  que lista nomes e não valores.
- Se o usuário colar uma senha na conversa, não a grave em nenhum arquivo:
  peça que ele mesmo a coloque no `.env`.

## Ambiente

- Use o conda env `mgi-notebooks` (`notebooks/environment.yml`). Dependência
  nova entra no `environment.yml`; não instale pacotes soltos com pip.

## Editando .ipynb

- Altere só as células pedidas e salve o JSON com `indent=1` e
  `ensure_ascii=False` para manter o diff pequeno. Não limpe saídas de outras
  células sem pedir.

## Antes de commitar

- Rode `betterleaks git . --pre-commit --staged -c .betterleaks.toml --redact`.
  Se acusar um segredo real, remova-o e avise o usuário. Nunca use
  `betterleaks:allow` para esconder um segredo real; ele serve só para falsos
  positivos, como o nome de uma variável.
