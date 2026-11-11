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
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python solucao_pyspark.py
```

> Os arquivos de dados não fazem parte do repositório. Copie-os para `data/` antes de executar.