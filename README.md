# itau-credit-risk-data-engineer

Case para Engenheiro de Dados Júnior (Risco de Crédito).

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

- Python 3.10 ou 3.11
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

## Avisos conhecidos no Windows

Ao executar localmente no Windows, o Spark exibe mensagens que **não afetam os resultados**:

- `WARN Shell: Did not find winutils.exe` / `HADOOP_HOME ... unset`: ferramentas do Hadoop usadas para gravar arquivos. A solução não persiste dados.
- `WARN NativeCodeLoader`: biblioteca nativa do Hadoop indisponível; o Spark usa a implementação em Java.
- `ERROR ShutdownHookManager ... NoSuchFileException`: falha ao remover a pasta temporária do Spark no encerramento. Ocorre após a execução.

Para visualizar apenas os resultados:

```powershell
python solucao_pyspark.py 2>$null
```