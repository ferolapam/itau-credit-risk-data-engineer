# itau-credit-risk-data-engineer

Case para Engenheiro de Dados Júnior (Risco de Crédito).

Solução em PySpark e Spark SQL para validação de qualidade de dados, agregações e consultas sobre uma base de e-commerce (10.001 clientes e 550.921 pedidos), com foco em **preservar o dado de origem, sinalizar problemas e manter a rastreabilidade**.

## Estrutura

```
.
├── data/                 # dados do case (não versionados)
│   ├── clients/data.json
│   └── pedidos/data.json
├── solucao_pyspark.py    # Seções 1 e 2 (PySpark e Spark SQL)
├── respostas.txt         # Seções 3 e 4 (Git e Code Review)
├── requirements.txt
└── README.md
```

## Pré-requisitos

- Python 3.8 a 3.11 (faixa suportada pelo PySpark 3.5.0)
- Java 17 (exigido pelo Spark)
- PySpark 3.5.0

## Como executar

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python solucao_pyspark.py
```

> Os arquivos de dados não fazem parte do repositório. Copie-os para `data/` antes de executar.

## Princípio da solução

Nenhum registro é removido ou corrigido. Cada problema de qualidade vira uma **flag** (`dq_*`) ao lado do dado, o **dado original é preservado** ao lado do dado tratado (`value_raw` e `value`, `name` e `name_treated`), e cada registro carrega sua **linhagem**. A decisão de como usar cada registro fica com quem consome a informação, e as causas dos problemas podem ser investigadas na origem.

## Arquitetura

| Camada | Implementação | Conteúdo |
|---|---|---|
| Bronze | `load_data` | leitura com schema explícito e colunas de linhagem (`_source_file`, `_ingested_at`, `_record_hash`) |
| Silver | `build_orders_quality` e `add_value_category` | visão tratada de pedidos: valor original e tratado, flags de qualidade, `is_valid` e `value_category` |
| Gold | `build_order_issues`, `build_client_report` e consultas SQL | saídas das Seções 1.1, 1.2, 1.3 e 2 |

A "quarentena" de registros inválidos é um filtro sobre a camada Silver (`is_valid = false`), que mantém cada registro no contexto dos demais (por exemplo, todos os pedidos que compartilham o mesmo id).

## Decisões de qualidade de dados

- **Schema explícito.** O campo `value` é lido como texto: 20 pedidos possuem `"N/A"`, o que fazia a inferência automática tipar a coluna inteira como string. A conversão para `decimal(10,2)` é feita depois, de forma controlada, distinguindo valor ausente de valor em formato inválido.
- **Decimal para valores monetários**, evitando erros de arredondamento do ponto flutuante.
- **Uma flag por regra**, para que um pedido registre todos os seus problemas (e não apenas o primeiro). Nenhuma flag resulta em nulo.
- **Regra de composição única** (`VALID_AMOUNT_SQL`): os totais consideram pedidos com valor numérico até 99.99, e a regra é compartilhada pelo PySpark e pelo SQL. Valores negativos permanecem na soma (possíveis estornos) e seguem sinalizados. A regra reproduz o exemplo do enunciado (cliente 9047: 65 pedidos e 4163.15).
- **Linhagem por registro.** O `_record_hash` (SHA-256 do conteúdo original) identifica cada pedido mesmo quando o id de negócio se repete.
- **Nome tratado ao lado do original.** Os filtros de texto usam `name_treated` (sem acento, sem espaço nas pontas e em maiúsculas); a saída mostra o nome original, e `dq_name_treated` indica os nomes alterados pelo tratamento.
- **Agrupamento por `client_id`**, e não por nome: há nomes iguais em clientes diferentes.
- **Ordenação determinística** em todas as saídas.
- **Conferências a cada execução**: contagem antes e depois dos joins, unicidade dos hashes e verificações dos filtros de texto.

## Resultados

| Seção | Resultado |
|---|---|
| 1.1 Validação de qualidade | 108.806 problemas em 71.217 pedidos (479.704 válidos) |
| 1.2 Compras por cliente | top 20 por `total_value`, com colunas de preservação e sinalização de volume atípico |
| 1.3 Categoria de valor | Low, Medium e High para todo pedido com valor numérico; distribuição exibida por flag |
| 2.1 Mais de 50 pedidos | 6.036 clientes (7.274 contando todos os pedidos, em `order_count_raw`) |
| 2.2 Top 10 por valor | idêntico ao top 10 da Seção 1.2 (validação cruzada entre as APIs) |
| 2.3 Filtros compostos | 3.794 pedidos |

Problemas por tipo (Seção 1.1):

| `issue_type` | Pedidos |
|---|---|
| `duplicated_id` (bônus) | 58.861 |
| `missing_value` | 24.981 |
| `invalid_range` | 24.924 |
| `invalid_format` (bônus) | 20 |
| `invalid_client` | 20 |

Cada saída é exibida no formato pedido pelo enunciado. Nas Seções 1.2, 2.1 e 2.3, ela é seguida de uma **visão de preservação**, com colunas adicionais que mostram os pedidos fora dos totais e as sinalizações de qualidade.

Os resultados foram conferidos contra cálculos independentes em Python.

## Achados e pontos de atenção

- **Ids de pedido repetidos não são cópias.** 26.258 ids aparecem em mais de um pedido, com cliente e valor diferentes, e todos os 550.921 registros têm hashes distintos. Em ~71% dos grupos há exatamente um pedido válido e os demais inválidos, o que sugere registros corrompidos reaproveitando o id de um pedido real. Os grupos com mais de um pedido válido podem indicar colisão na geração de ids ou concorrência na finalização da compra. A base não possui data/hora nem produto para confirmar a causa.
- **Cliente 123456.** Id fora do padrão (os demais vão de 0 a 9.999), nome igual ao de outro cliente e 400 pedidos, contra uma mediana de 55. Permanece nos relatórios, sinalizado por `dq_volume_outlier` (regra de Tukey: Q3 + 3 × IQR).
- **Nomes repetidos entre clientes.** "Ana Silva" corresponde a 100 clientes diferentes, com ids múltiplos de 100.
- **Valores fora da faixa.** 24.904 valores negativos e 20 valores acima de 99.99 (até 4.874,84).
- **Pedidos sem cliente.** 20 pedidos referenciam os ids 999991, 999992 e 999993, inexistentes no cadastro.

## Evolução para produção

- **Validação declarativa** com Great Expectations ou Soda Core, usando as regras atuais como contrato inicial.
- **Persistência em formato transacional** (Delta Lake ou Apache Iceberg), com a camada de quarentena gravada para auditoria.
- **Contratos de dados por domínio**: pedidos (Vendas) e clientes (Cadastro). O `invalid_client` representa uma quebra de integridade entre os dois produtos de dados.
- **Novos campos na origem** (data/hora, produto e status do pedido) para confirmar a causa dos ids repetidos.
- **Proteção da branch `main`**, exigindo Pull Request e revisão antes do merge.

## Fluxo de desenvolvimento

Cada entrega foi feita em uma branch `feature/*` e integrada por Pull Request, com commits no padrão Conventional Commits (`feat`, `docs`, `build`, `style`, `chore`). O `.gitignore` foi o primeiro commit do repositório, para que nenhum dado do case entrasse no histórico.

| PR | Entrega |
|---|---|
| #1 | estrutura do projeto |
| #2 | Seção 1.1: validação de qualidade de dados |
| #3 | Seção 1.2: relatório de compras por cliente |
| #4 | Seção 1.3: categoria de valor |
| #5 | Seção 2: consultas em Spark SQL |
| #6 | Seções 3 e 4: respostas de Git e Code Review |

Os Pull Requests seguintes trataram da documentação final, da conformidade com o enunciado e da declaração de uso de IA.

## Uso de inteligência artificial

Conforme permitido no enunciado, utilizei o assistente de IA **Claude (Anthropic)** como apoio na implementação do código PySpark e Spark SQL, na análise exploratória dos dados (com foco em identificar possíveis gargalos e riscos de qualidade), na redação das respostas e da documentação, e nos comandos de Git.

Como conduzi o trabalho:

- **Decisões de abordagem**: defini o princípio de não remover nem corrigir registros, mas preservar o dado original, sinalizar os problemas com flags e manter a rastreabilidade até que a causa seja investigada. Esse princípio orientou o tratamento dos ids duplicados, dos valores fora da faixa e do cliente 123456.
- **Revisão crítica das sugestões da IA**: identifiquei e rejeitei propostas que contrariavam o princípio de preservação, como excluir pedidos inválidos ou o cliente 123456 dos relatórios. Em vez de excluir, propus sinalizar com flags, mantendo o dado original ao lado do dado tratado (por exemplo, `value_raw` e `value`, e a flag `dq_name_treated` ao lado de `name_treated`).
- **Exigência de evidência no código**: as verificações de qualidade (nomes com acento, espaços, duplicações, valores acima do limite) são executadas pelo próprio código a cada rodada, e não apenas em análises externas.
- **Hipóteses testadas nos dados**: hipóteses como a de concorrência na geração de ids foram confrontadas com os dados antes de entrar na documentação.
- **Execução e validação**: executei cada etapa localmente, conferi os resultados contra cálculos independentes, revisei cada Pull Request e corrigi os problemas encontrados na revisão.

Compreendo as escolhas técnicas desta solução e estou à disposição para discuti-las.

## Avisos conhecidos no Windows

Ao executar localmente no Windows, o Spark exibe mensagens que **não afetam os resultados**:

- `WARN Shell: Did not find winutils.exe` / `HADOOP_HOME ... unset`: ferramentas do Hadoop usadas para gravar arquivos. A solução não persiste dados.
- `WARN NativeCodeLoader`: biblioteca nativa do Hadoop indisponível; o Spark usa a implementação em Java.
- `ERROR ShutdownHookManager ... NoSuchFileException`: falha ao remover a pasta temporária do Spark no encerramento. Ocorre após a execução.

Para visualizar apenas os resultados:

```powershell
python solucao_pyspark.py 2>$null
```
