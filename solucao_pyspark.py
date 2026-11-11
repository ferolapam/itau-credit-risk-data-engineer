"""
Case - Engenheiro de Dados Júnior
Seções 1 e 2: PySpark e Spark SQL
"""

import os
import sys
from decimal import Decimal
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

# No Windows, o Spark procura um executável chamado "python3", que não existe no venv.
# Define explicitamente o Python que executa este script (o do .venv), garantindo
# que o driver e os workers do Spark usem a mesma versão (3.11).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CLIENTS_PATH = "data/clients/data.json"
ORDERS_PATH = "data/pedidos/data.json"

# Valores monetários usam decimal (precisão exata) em vez de double (ponto flutuante),
# evitando erros de arredondamento em somas.
MONEY_TYPE = DecimalType(10, 2)

# Regra do case: value deve ser maior que 0 e menor ou igual a 99.99.
VALUE_MIN_EXCLUSIVE = Decimal("0")
VALUE_MAX_INCLUSIVE = Decimal("99.99")

# Flags de qualidade de dados aplicadas a cada pedido.
DQ_COLUMNS = [
    "dq_missing_id",
    "dq_missing_client_id",
    "dq_missing_value",
    "dq_invalid_format",
    "dq_invalid_range",
    "dq_client_not_found",
    "dq_duplicated_id",
]

# Colunas técnicas de linhagem (prefixo "_"): registram origem, momento da leitura
# e identidade de cada registro, independentemente dos identificadores de negócio.
LINEAGE_COLUMNS = ["_source_file", "_ingested_at", "_record_hash"]

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


def add_lineage(df: DataFrame, raw_columns: list[str]) -> DataFrame:
    """
    Adiciona colunas de linhagem a um DataFrame recém-lido:
    - _source_file: arquivo de origem do registro;
    - _ingested_at: data e hora da leitura;
    - _record_hash: impressão digital (SHA-256) do conteúdo original do registro.

    O hash identifica cada registro mesmo quando o id de negócio não é único,
    preservando a rastreabilidade até a origem.
    """
    # Nulos recebem um marcador explícito antes do hash: sem isso, um campo nulo
    # e um campo vazio gerariam o mesmo conteúdo e, portanto, o mesmo hash.
    hash_input = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in raw_columns],
    )
    return (
        df
        # Mantém apenas o caminho relativo (ex.: data/pedidos/data.json), sem expor
        # o diretório da máquina em que o código foi executado.
        .withColumn("_source_file", F.regexp_extract(F.input_file_name(), r"(data/.*)$", 1))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(hash_input, 256))
    )


def load_data(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Carrega clientes e pedidos aplicando os schemas explícitos e a linhagem dos pedidos."""
    clients_df = spark.read.schema(CLIENTS_SCHEMA).json(CLIENTS_PATH)
    orders_df = add_lineage(
        spark.read.schema(ORDERS_SCHEMA).json(ORDERS_PATH),
        raw_columns=["id", "client_id", "value"],
    )
    return clients_df, orders_df


def build_orders_quality(orders_df: DataFrame, clients_df: DataFrame) -> DataFrame:
    """
    Gera a visão tratada de pedidos: preserva o valor original (value_raw), adiciona
    o valor tratado (value) e uma flag booleana para cada problema de qualidade.
    Nenhum registro é removido ou corrigido: a decisão de uso fica com o consumidor.
    """
    clients_lookup = clients_df.select(
        F.col("id").alias("client_ref_id"),
        F.col("name").alias("client_name"),
    )

    # Janela por id de pedido: permite contar quantas vezes cada id aparece
    # sem agrupar (e, portanto, sem perder) as linhas.
    orders_by_id = Window.partitionBy("id")

    value = F.col("value")
    value_raw = F.col("value_raw")

    enriched_df = (
        orders_df
        .withColumnRenamed("value", "value_raw")
        # Valores não numéricos (ex.: "N/A") resultam em nulo na conversão.
        .withColumn("value", value_raw.cast(MONEY_TYPE))
        # Left join: mantém todos os pedidos, inclusive os de clientes inexistentes.
        .join(clients_lookup, F.col("client_id") == F.col("client_ref_id"), "left")
        .withColumn("dq_missing_id", F.col("id").isNull())
        .withColumn("dq_missing_client_id", F.col("client_id").isNull())
        .withColumn("dq_missing_value", value_raw.isNull())
        # Valor presente na origem, mas que não pôde ser convertido para número.
        .withColumn("dq_invalid_format", value_raw.isNotNull() & value.isNull())
        .withColumn(
            "dq_invalid_range",
            value.isNotNull()
            & ((value <= VALUE_MIN_EXCLUSIVE) | (value > VALUE_MAX_INCLUSIVE)),
        )
        .withColumn(
            "dq_client_not_found",
            F.col("client_id").isNotNull() & F.col("client_ref_id").isNull(),
        )
        # Ids repetidos não são cópias: o mesmo id aparece com cliente e valor
        # diferentes. Em ~71% dos grupos há exatamente um pedido válido e os demais
        # inválidos, o que sugere registros corrompidos reaproveitando o id de um
        # pedido real. Os grupos com mais de um pedido válido podem indicar colisão
        # na geração de ids ou concorrência na finalização da compra; a base não
        # possui data/hora nem produto para confirmar. Todas as linhas do grupo são
        # marcadas, pois não há como determinar qual delas é a correta.
        .withColumn(
            "dq_duplicated_id",
            F.col("id").isNotNull() & (F.count(F.lit(1)).over(orders_by_id) > 1),
        )
    )

    # Um pedido é válido quando nenhuma flag de qualidade foi acionada.
    any_issue = reduce(lambda left, right: left | right, [F.col(c) for c in DQ_COLUMNS])

    return (
        enriched_df
        .withColumn("is_valid", ~any_issue)
        .select(
            "id", "client_id", "client_name", "value_raw", "value",
            *DQ_COLUMNS, "is_valid", *LINEAGE_COLUMNS,
        )
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    clients_df, orders_df = load_data(spark)

    orders_quality_df = build_orders_quality(orders_df, clients_df)
    # A visão tratada é reutilizada em várias consultas: mantê-la em memória
    # evita reprocessar a leitura, o join e a janela a cada uso.
    orders_quality_df.cache()

    # Conferência: o left join com clientes não pode duplicar nem perder pedidos.
    print(f"Pedidos lidos: {orders_df.count()} | Pedidos na visão tratada: {orders_quality_df.count()}")

    # Conferência: se todo registro tem um hash único, não existem cópias exatas;
    # os ids duplicados são, portanto, pedidos diferentes compartilhando o mesmo id.
    distinct_hashes = orders_quality_df.select("_record_hash").distinct().count()
    print(f"Hashes distintos: {distinct_hashes} de {orders_quality_df.count()} registros")

    print("Quantidade de pedidos por flag de qualidade:")
    orders_quality_df.select(
        [F.sum(F.col(c).cast("int")).alias(c) for c in DQ_COLUMNS]
    ).show(vertical=True)

    print("Pedidos válidos x inválidos:")
    orders_quality_df.groupBy("is_valid").count().show()

    print("Exemplos de pedidos com problema:")
    (
        orders_quality_df
        .filter(~F.col("is_valid"))
        .select("id", "client_id", "client_name", "value_raw", "value", *DQ_COLUMNS, "is_valid")
        .orderBy("id")
        .show(10, truncate=False)
    )

    print("Rastreabilidade de pedidos que compartilham o mesmo id:")
    (
        orders_quality_df
        .filter(F.col("dq_duplicated_id"))
        .select("id", "client_id", "value_raw", *LINEAGE_COLUMNS)
        .orderBy("id", "client_id")
        .show(7, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()