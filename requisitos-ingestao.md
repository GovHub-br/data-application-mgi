# Requisitos de Ingestão — API Dados Abertos Compras Gov.br

**Base URL:** `https://dadosabertos.compras.gov.br`  
**Documentação:** `https://dadosabertos.compras.gov.br/v3/api-docs`  

---

## 1. Regras Gerais

### 1.1 Rate Limiting

O limite exato de requisições por minuto da API não é documentado. A estratégia adotada que se mostrou estável:

- **1 segundo de intervalo** entre requisições consecutivas dentro de um mesmo bloco
- **Blocos de 15 páginas** por task, processadas sequencialmente
- **Timeout de leitura de 120 segundos** (a API apresenta alta latência em alguns endpoints, especialmente contratações)

### 1.2 Parâmetros de Data (conf)

DAGs com filtro de data aceitam um intervalo via configuração do DAG run. Na interface do Airflow, usar **Trigger DAG w/ config** e passar:

```json
{
  "data_inicial": "2025-01-01",
  "data_final": "2025-12-31"
}
```

Se nenhum conf for passado, ambas as datas assumem o valor de `ds` (data de execução do dia) — comportamento padrão para execuções diárias agendadas.

DAGs que suportam este parâmetro: `contratacoes_dag`, `itens_contratacoes_dag`, `resultado_itens_contratacoes_dag`, `contratos_dag`, `contratos_item_dag`, `arp_dag`, `arp_item_dag`, `arp_detalhes_dag`.

---

### 1.3 Paginação

Todos os endpoints paginados retornam no corpo da resposta:

```json
{
  "totalRegistros": 734217,
  "totalPaginas": 73422,
  "paginasRestantes": 73421
}
```

**Requisito:** A primeira requisição deve usar `pagina=1`. Em seguida, iterar incrementando `pagina` até `paginasRestantes == 0`.  
**Tamanho de página recomendado:** `tamanhoPagina=500` (máximo aceito pela API).  

---

## 2. Módulos a Ingerir

### 2.1 Catálogo de Materiais (`/modulo-material`)

Hierarquia: **Grupo → Classe → PDM → Item**  
Tabelas de apoio: Natureza de Despesa, Unidade de Fornecimento, Características.

| # | Endpoint | Filtros obrigatórios | Tipo de carga | DAG | PK |
|---|----------|----------------------|---------------|-----|----|
| 1 | `1_consultarGrupoMaterial` | `statusGrupo=true` | Full/Incremental | `catalogo_material_grupo_classe_dag` | `codigogrupo` |
| 2 | `2_consultarClasseMaterial` | `statusClasse=true` | Full/Incremental | `catalogo_material_grupo_classe_dag` | `codigoclasse` |
| 3 | `3_consultarPdmMaterial` | `statusPdm=true` | Full/Incremental | `catalogo_material_pdm_dag` | `codigopdm` |
| 4 | `4_consultarItemMaterial` | `statusItem=true` | Full/Incremental | `catalogo_material_item_dag` | `codigoitem` |
| 5 | `5_consultarMaterialNaturezaDespesa` | `statusNaturezaDespesa=true` | Full | `catalogo_material_natureza_despesa_dag` | sem PK* |
| 6 | `6_consultarMaterialUnidadeFornecimento` | `statusUnidadeFornecimentoPdm=true` | Full | `catalogo_material_unidade_fornecimento_dag` | sem PK* |
| 7 | `7_consultarMaterialCaracteristicas` | — | Full | `catalogo_material_caracteristicas_dag` | sem PK* |

**Implementação:** cada DAG usa `get_page_starts` (descobre total de páginas) + `fetch_block.expand()` em blocos de 15 páginas + `validate` (compara total ingerido com `totalRegistros` da API).

*Sem PK por problemas de qualidade nos dados retornados pela API — campos-chave chegam nulos ou como `NaN`. Pendente investigação.

---

### 2.2 Catálogo de Serviços (`/modulo-servico`)

Hierarquia: **Seção → Divisão → Grupo → Classe → Subclasse → Item**  
Tabelas de apoio: Unidade de Medida, Natureza de Despesa.

| # | Endpoint | Filtros obrigatórios | Tipo de carga | DAG | PK |
|---|----------|----------------------|---------------|-----|----|
| 1 | `1_consultarSecaoServico` | `statusSecao=true` | Full/Incremental | `catalogo_servico_hierarquia_dag` | `codigosecao` |
| 2 | `2_consultarDivisaoServico` | `statusDivisao=true` | Full/Incremental | `catalogo_servico_hierarquia_dag` | `codigodivisao` |
| 3 | `3_consultarGrupoServico` | `statusGrupo=true` | Full/Incremental | `catalogo_servico_hierarquia_dag` | `codigogrupo` |
| 4 | `4_consultarClasseServico` | — | Full/Incremental | `catalogo_servico_hierarquia_dag` | `codigoclasse` |
| 5 | `5_consultarSubClasseServico` | `statusSubclasse=true` | Full/Incremental | `catalogo_servico_hierarquia_dag` | `codigosubclasse` |
| 6 | `6_consultarItemServico` | `statusServico=true` | Full/Incremental | `catalogo_servico_item_dag` | `codigoservico` |
| 7 | `7_consultarUndMedidaServico` | `statusUnidadeMedida=true` | Full/Incremental | `catalogo_servico_und_medida_dag` | `codigoservico + siglaunidademedida` |
| 8 | `8_consultarNaturezaDespesaServico` | `statusNaturezaDespesa=true` | Full/Incremental | `catalogo_servico_natureza_despesa_dag` | `codigoservico + codigonaturezadespesa` |

**Implementação:** endpoints 1–5 (hierarquia) em uma única DAG com tasks separadas por endpoint via `.override(task_id=...)`. Endpoints 6–8 em DAGs individuais. Todas usam `get_page_starts` + `fetch_block.expand()` em blocos de 15 páginas + `validate`.

---

### 2.3 UASG — Unidades Administrativas (`/modulo-uasg`)

| # | Endpoint | Filtros obrigatórios | Tipo de carga | DAG | PK |
|---|----------|----------------------|---------------|-----|----|
| 1 | `1_consultarUasg` | `statusUasg=true` | Full/Incremental | `uasg_dag` | `codigouasg` |
| 2 | `2_consultarOrgao` | `statusOrgao=true` | Full/Incremental | `orgao_dag` | `codigoorgao` |

**Implementação:** DAGs separadas com `get_page_starts` + `fetch_block.expand()` em blocos de 15 páginas + `validate`. Ao finalizar, `orgao_dag` dispara `contratos_dag` via `TriggerDagRunOperator` pois `contratos_dag` depende de `raw_orgao`.

---

### 2.4 Contratações Lei 14.133/2021 (`/modulo-contratacoes`)

Fluxo: **Contratação (Edital) → Itens → Resultados dos Itens**

| # | Endpoint | Parâmetros obrigatórios | Tipo de carga | DAG | PK |
|---|----------|-------------------------|---------------|-----|----|
| 1 | `1_consultarContratacoes_PNCP_14133` | `dataPublicacaoPncpInicial`, `dataPublicacaoPncpFinal`, `codigoModalidade` | Incremental diária | `contratacoes_dag` | `idcompra` |
| 2 | `2_consultarItensContratacoes_PNCP_14133` | `dataInclusaoPncpInicial`, `dataInclusaoPncpFinal` | Incremental diária | `itens_contratacoes_dag` | `idcompraitem` |
| 3 | `3_consultarResultadoItensContratacoes_PNCP_14133` | `dataResultadoPncpInicial`, `dataResultadoPncpFinal` | Incremental diária | `resultado_itens_contratacoes_dag` | `idcompraitem + sequencialresultado` |

**Modalidades (endpoint 1):** `3` = Concorrência Eletrônica, `5` = Pregão Eletrônico, `6` = Dispensa, `7` = Inexigibilidade

**Implementação:** 3 DAGs independentes, mesma schedule (`0 4 * * *`). Endpoint 1 itera as 4 modalidades sequencialmente (m3 → m5 → m6 → m7) com `expand` em blocos de 15 páginas. Endpoints 2 e 3 com o mesmo padrão, sem dependência entre si. Datas via conf (`data_inicial`, `data_final`); padrão é o `ds` do dia.

**Atenção:** endpoints apresentam alta latência — timeout de leitura configurado em 300s.

---

### 2.5 Contratos (`/modulo-contratos`)

Registra os contratos formalizados após a contratação. Complementa o módulo 2.4.

| # | Endpoint | Parâmetros obrigatórios | Tipo de carga | DAG | PK |
|---|----------|-------------------------|---------------|-----|----|
| 1 | `1_consultarContratos` | `codigoOrgao`, `dataVigenciaInicialMin`, `dataVigenciaInicialMax` | Incremental por órgão/período | `contratos_dag` | `codigounidadegestora + numerocontrato + nifornecedor` |
| 2 | `2_consultarContratosItem` | `codigoOrgao`, `dataVigenciaInicialMin`, `dataVigenciaInicialMax` | Incremental por órgão/período | `contratos_item_dag` | `codigounidadegestora + numerocontrato + nifornecedor + numeroitem + contratoitemexcluido` |

**Implementação:** `codigoOrgao` é obrigatório — a ingestão itera sobre os órgãos de `raw_orgao` via `get_orgaos` + `ingest_orgao.expand(codigo_orgao=orgaos)`. `max_active_tis_per_dag=4` para limitar requisições simultâneas à API. Datas via conf (`data_inicial`, `data_final`); padrão é o `ds` do dia.

---

### 2.6 ARP — Atas de Registro de Preços (`/modulo-arp`)

Registra as atas de registro de preços (resultado de pregões/concorrências para compras futuras).

| # | Endpoint | Parâmetros obrigatórios | Tipo de carga | DAG | PK |
|---|----------|-------------------------|---------------|-----|----|
| 1 | `1_consultarARP` | `dataVigenciaInicialMin`, `dataVigenciaInicialMax` | Incremental por período | `arp_dag` | `numeroaregistropreco + codigounidadegerenciadora` |
| 2 | `2_consultarARPItem` | `dataVigenciaInicialMin`, `dataVigenciaInicialMax` | Incremental por período | `arp_item_dag` | `numeroaregistropreco + codigounidadegerenciadora + numeroitem` |
| 3 | `3_consultarUnidadesItem` | `numeroAta`, `unidadeGerenciadora`, `numeroItem` | Detalhe por item | `arp_detalhes_dag` | sem PK* |
| 4 | `4_consultarEmpenhosSaldoItem` | `numeroAta`, `unidadeGerenciadora` | Detalhe financeiro | `arp_detalhes_dag` | sem PK* |
| 5 | `5_consultarAdesoesItem` | `numeroAta`, `unidadeGerenciadora`, `numeroItem` | Adesões (carona) | `arp_detalhes_dag` | sem PK* |

**Implementação:** Endpoints 1 e 2 usam `get_page_starts` + `fetch_block.expand()` em blocos de 15 páginas + `validate`. Endpoints 3, 4 e 5 em `arp_detalhes_dag`: lê itens/pares de `raw_arp_item` filtrados por data, depois `fetch_unidades_adesoes.expand(args=items)` (endpoints 3+5 por item) e `fetch_empenhos.expand(args=pares)` (endpoint 4 por ata). `max_active_tis_per_dag=4`. Datas via conf (`data_inicial`, `data_final`).

*PK a definir após primeira ingestão.

---

### 2.7 Fornecedores (`/modulo-fornecedor`)

Cadastro de fornecedores participantes de licitações.

| # | Endpoint | Filtros obrigatórios | Tipo de carga | DAG | PK |
|---|----------|----------------------|---------------|-----|----|
| 1 | `1_consultarFornecedor` | `ativo=true` | Full/Incremental | `fornecedor_dag` | `nifornecedor`* |

**Implementação:** Padrão full catalog — `get_page_starts` + `fetch_block.expand()` em blocos de 15 páginas + `validate`. Sem parâmetros de data; filtra apenas `ativo=true`. Schedule `0 2 * * *`.

**Observação:** Pode ser usado para enriquecer registros de resultados de contratações com dados do fornecedor.

*PK inferida — confirmar após primeira ingestão.

---

### 2.8 Pesquisa de Preços (`/modulo-pesquisa-preco`)

Histórico de preços praticados nas compras públicas — base para formação de preços de referência.

| # | Endpoint | Parâmetros obrigatórios | Tipo de carga | DAG | PK |
|---|----------|-------------------------|---------------|-----|----|
| 1 | `1_consultarMaterial` | `codigoItemCatalogo` | Por item de catálogo | `pesquisa_preco_material_dag` | sem PK* |
| 2 | `2_consultarMaterialDetalhe` | `codigoItemCatalogo` | Detalhamento por item | `pesquisa_preco_material_dag` | sem PK* |
| 3 | `3_consultarServico` | `codigoItemCatalogo` | Por item de catálogo | `pesquisa_preco_servico_dag` | sem PK* |
| 4 | `4_consultarServicoDetalhe` | `codigoItemCatalogo` | Detalhamento por item | `pesquisa_preco_servico_dag` | sem PK* |

**Implementação:** Dois DAGs — um para material (endpoints 1+2) e um para serviço (endpoints 3+4). Cada DAG lê os itens já ingeridos do catálogo (`raw_item_material` ou `raw_item_servico`) e expande via `fetch_preco.expand(codigo_item=itens)`, chamando os dois endpoints dentro de cada task. `max_active_tis_per_dag=4`. Schedule semanal (`0 3 * * 0`) por ser carga pesada.

**Dependência:** requer `raw_item_material` (de `catalogo_material_item_dag`) e `raw_item_servico` (de `catalogo_servico_item_dag`) populados antes de executar.

*PK a definir após primeira ingestão.

---

## 3. Dependências entre Módulos

```
modulo-uasg (órgãos/UASGs)
    └─► modulo-contratos (codigoOrgao obrigatório)

modulo-material (itens de catálogo)
    └─► modulo-pesquisa-preco (codigoItemCatalogo obrigatório para material)

modulo-servico (itens de catálogo)
    └─► modulo-pesquisa-preco (codigoItemCatalogo obrigatório para serviço)

modulo-contratacoes (idCompra)
    └─► modulo-arp (via contratação origem)
    └─► modulo-contratos (via contratação origem)
```

## 4. Requisitos Técnicos Mínimos

### 4.1 Paginação
- Sempre iniciar com `pagina=1` e `tamanhoPagina=500`
- Ler `totalPaginas` da primeira resposta e iterar até cobrir todas as páginas
- Registrar `totalRegistros` para validação de completude

### 4.2 Carga Incremental
- Para endpoints com parâmetros de data, usar janelas diárias (ex: D-1)
- Guardar a data da última carga bem-sucedida para retomada em caso de falha
- Para endpoints com `dataAtualizacaoPncp`, usar para detectar registros atualizados

### 4.3 Tratamento de erros
- Implementar retry com backoff exponencial (mínimo 3 tentativas)
- Registrar e alertar falhas por endpoint
- Não interromper a ingestão de um módulo por falha em uma página — registrar e continuar

### 4.4 Controle de qualidade
- Comparar `totalRegistros` retornado pela API com a contagem efetiva ingerida
- Validar presença de campos-chave (ex: `idCompra`, `codigoOrgao`) antes de persistir
- Registrar timestamp de ingestão em cada registro

### 4.5 Formato
- Todos os endpoints retornam JSON por padrão
- Datas na API seguem o formato `YYYY-MM-DD`

