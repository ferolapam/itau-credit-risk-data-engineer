"""
Case - Engenheiro de Dados Júnior
Seções 1 e 2: PySpark e Spark SQL
"""

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# No Windows, o Spark procura um executável chamado "python3", que não existe no venv.
# Define explicitamente o Python que executa este script (o do .venv), garantindo
# que o driver e os workers do Spark usem a mesma versão (3.11).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CLIENTS_PATH = "data/clients/data.json"
ORDERS_PATH = "data/pedidos/data.json"

# Schemas explícitos: os tipos não dependem do conteúdo do arquivo e o Spark
# não precisa ler os dados duas vezes para inferir a estrutura.
CLIENTS_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])

# "value" é lido como texto de propósito: existem registros com valores não numéricos
# (ex.: "N/A"). Ler como texto preserva o dado bruto, e a conversão para número é feita
# depois, de forma controlada, permitindo distinguir valor ausente de valor inválido.
ORDERS_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("client_id", LongType(), True),
    StructField("value", StringType(), True),
])


def create_spark_session() -> SparkSession:
    """Cria a sessão Spark local, usando todos os núcleos disponíveis."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("case_engenheiro_dados_junior")
        .getOrCreate()
    )


def load_data(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Carrega clientes e pedidos aplicando os schemas explícitos."""
    clients_df = spark.read.schema(CLIENTS_SCHEMA).json(CLIENTS_PATH)
    orders_df = spark.read.schema(ORDERS_SCHEMA).json(ORDERS_PATH)
    return clients_df, orders_df


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    clients_df, orders_df = load_data(spark)

    # Exploração: valores de "value" que existem, mas não são numéricos.
    print("Valores não numéricos em pedidos.value:")
    (
        orders_df
        .filter(F.col("value").isNotNull() & F.col("value").cast("double").isNull())
        .groupBy("value")
        .count()
        .show()
    )

    spark.stop()


if __name__ == "__main__":
    main()