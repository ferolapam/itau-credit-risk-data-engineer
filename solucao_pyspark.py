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

# Formato exigido pela Seção 1.2 para o total de compras por cliente.
TOTAL_VALUE_TYPE = DecimalType(11, 2)

# Limites da Seção 1.3: Low (< 20.00), Medium (>= 20.00 e < 50.00), High (>= 50.00).
CATEGORY_MEDIUM_MIN = Decimal("20.00")
CATEGORY_HIGH_MIN = Decimal("50.00")

# Fator da regra de Tukey (Q3 + fator x IQR) para sinalizar clientes com volume de
# pedidos fora do padrão. O fator 3 identifica apenas valores extremos ("far out").
OUTLIER_IQR_FACTOR = 3

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


def build_order_issues(orders_quality_df: DataFrame) -> DataFrame:
    """
    Seção 1.1: converte a visão tratada (uma linha por pedido, com flags) para o
    formato pedido pelo enunciado (uma linha por problema encontrado).

    As regras de qualidade ficam apenas em build_orders_quality: esta função só lê
    as flags, garantindo que as duas saídas nunca divirjam.
    """
    # Cada regra associa uma flag ao tipo de problema e à sua descrição.
    # As descrições usam value_raw (o dado como chegou da origem), pois é esse o
    # valor que precisa ser localizado e corrigido no sistema de origem.
    issue_rules = [
        ("dq_missing_id", "missing_value", F.lit("Campo id está nulo")),
        ("dq_missing_client_id", "missing_value", F.lit("Campo client_id está nulo")),
        ("dq_missing_value", "missing_value", F.lit("Campo value está nulo")),
        # Bônus: valor presente, mas não numérico (ex.: "N/A").
        ("dq_invalid_format", "invalid_format",
         F.format_string("Valor '%s' não é numérico", "value_raw")),
        ("dq_invalid_range", "invalid_range",
         F.format_string("Valor %s fora do range permitido", "value_raw")),
        ("dq_client_not_found", "invalid_client",
         F.format_string("client_id %s não existe", "client_id")),
        # Bônus: o mesmo id é usado por pedidos diferentes. A descrição inclui
        # client_id e value para que cada linha aponte para um pedido específico.
        ("dq_duplicated_id", "duplicated_id",
         F.format_string(
             "Id repetido em outro pedido (client_id %s, value %s)",
             "client_id",
             F.coalesce(F.col("value_raw"), F.lit("nulo")),
         )),
    ]

    # Garante que toda flag de qualidade tenha uma regra de saída correspondente.
    assert [flag for flag, _, _ in issue_rules] == DQ_COLUMNS, "Flag sem regra de saída"

    issues_by_rule = [
        orders_quality_df
        .filter(F.col(flag))
        .select(
            F.col("id").alias("pedido_id"),
            F.lit(issue_type).alias("issue_type"),
            description.alias("issue_description"),
        )
        for flag, issue_type, description in issue_rules
    ]

    # Ordena por pedido_id (exigido) e, em seguida, por tipo e descrição, para que
    # a saída seja sempre a mesma em qualquer execução.
    return (
        reduce(DataFrame.unionByName, issues_by_rule)
        .orderBy("pedido_id", "issue_type", "issue_description")
    )


def build_client_report(orders_quality_df: DataFrame) -> tuple[DataFrame, float]:
    """
    Seção 1.2: relatório de compras por cliente.

    Colunas exigidas pelo enunciado:
    - total_orders / total_value: pedidos cujo valor é numérico e não ultrapassa 99.99.
      Valores negativos são mantidos na soma (podem representar estornos ou devoluções)
      e continuam sinalizados como problema de qualidade na Seção 1.1.

    Colunas adicionais, para que nenhum pedido deixe de ser visível no relatório:
    - total_orders_raw: todos os pedidos do cliente, sem qualquer filtro;
    - orders_with_issues: pedidos do cliente com ao menos uma flag de qualidade;
    - dq_volume_outlier: cliente com volume de pedidos fora do padrão estatístico.

    Retorna o relatório e o limite de volume usado para a sinalização.
    """
    # Pedido considerado no total: valor numérico e dentro do limite superior.
    in_total = F.col("value").isNotNull() & (F.col("value") <= VALUE_MAX_INCLUSIVE)

    per_client_df = (
        orders_quality_df
        # Pedidos de clientes inexistentes não têm a quem ser atribuídos no relatório
        # e já estão reportados na Seção 1.1 (invalid_client).
        .filter(~F.col("dq_client_not_found"))
        # Agrupa por client_id (e não por nome): há nomes iguais em clientes diferentes.
        .groupBy("client_id", "client_name")
        .agg(
            F.count(F.when(in_total, True)).alias("total_orders"),
            F.sum(F.when(in_total, F.col("value"))).cast(TOTAL_VALUE_TYPE).alias("total_value"),
            F.count(F.lit(1)).alias("total_orders_raw"),
            F.count(F.when(~F.col("is_valid"), True)).alias("orders_with_issues"),
        )
        # Regra do enunciado: apenas clientes com pelo menos 1 pedido.
        .filter(F.col("total_orders") >= 1)
    )

    # Limite estatístico de volume (regra de Tukey), calculado a partir dos próprios
    # dados, sem valores fixados manualmente. relativeError=0 gera quartis exatos.
    q1, q3 = per_client_df.approxQuantile("total_orders_raw", [0.25, 0.75], 0)
    volume_threshold = q3 + OUTLIER_IQR_FACTOR * (q3 - q1)

    report_df = (
        per_client_df
        .withColumn("dq_volume_outlier", F.col("total_orders_raw") > volume_threshold)
        # Desempate por client_id para que o top 20 seja sempre o mesmo.
        .orderBy(F.desc("total_value"), F.asc("client_id"))
        .select(
            "client_id", "client_name", "total_orders", "total_value",
            "total_orders_raw", "orders_with_issues", "dq_volume_outlier",
        )
    )
    return report_df, volume_threshold


def add_value_category(orders_quality_df: DataFrame) -> DataFrame:
    """
    Seção 1.3: adiciona value_category à visão tratada, aplicando a regra do enunciado
    a todo pedido com valor numérico. Pedidos sem valor numérico ficam sem categoria.

    A regra é aplicada literalmente: valores negativos caem em "Low" e valores acima
    de 99.99 caem em "High". Esses pedidos continuam identificáveis pela flag
    dq_invalid_range, que fica ao lado da categoria na visão tratada.
    """
    value = F.col("value")
    # As faixas são mutuamente exclusivas, então o encadeamento de when() é seguro:
    # cada pedido pertence a exatamente uma categoria.
    return orders_quality_df.withColumn(
        "value_category",
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(value < CATEGORY_MEDIUM_MIN, F.lit("Low"))
        .when(value < CATEGORY_HIGH_MIN, F.lit("Medium"))
        .otherwise(F.lit("High")),
    )


def build_value_category_sample(orders_quality_df: DataFrame) -> DataFrame:
    """
    Seção 1.3: amostra de pedidos categorizados, ordenada por value decrescente.

    Usa a mesma composição da Seção 1.2 (valor numérico até 99.99). Sem esse critério,
    os 20 primeiros registros seriam exatamente os 20 valores acima do limite
    (ex.: 4874.84), e a amostra não representaria pedidos reais. Esses registros
    seguem na visão tratada, categorizados e sinalizados com dq_invalid_range.
    """
    return (
        orders_quality_df
        .filter(F.col("value").isNotNull() & (F.col("value") <= VALUE_MAX_INCLUSIVE))
        # Desempate por id e pelo hash do registro: há vários pedidos com o mesmo
        # valor (e ids repetidos), e a amostra precisa ser a mesma em toda execução.
        .orderBy(F.desc("value"), F.asc("id"), F.asc("_record_hash"))
        .select("id", "value", "value_category")
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    clients_df, orders_df = load_data(spark)

    orders_quality_df = add_value_category(build_orders_quality(orders_df, clients_df))
    # A visão tratada é reutilizada em várias consultas: mantê-la em memória
    # evita reprocessar a leitura, o join e a janela a cada uso.
    orders_quality_df.cache()

    print("=" * 80)
    print("CONFERÊNCIAS DE INTEGRIDADE")
    print("=" * 80)
    total_orders = orders_df.count()
    total_quality = orders_quality_df.count()
    # O left join com clientes não pode duplicar nem perder pedidos.
    print(f"Pedidos lidos: {total_orders} | Pedidos na visão tratada: {total_quality}")
    # Se todo registro tem um hash único, não existem cópias exatas: os ids
    # duplicados são pedidos diferentes compartilhando o mesmo id.
    distinct_hashes = orders_quality_df.select("_record_hash").distinct().count()
    print(f"Hashes distintos: {distinct_hashes} de {total_quality} registros")
    print("Pedidos válidos x inválidos:")
    orders_quality_df.groupBy("is_valid").count().orderBy("is_valid").show()

    print("=" * 80)
    print("SEÇÃO 1.1 - VALIDAÇÃO DE QUALIDADE DE DADOS")
    print("=" * 80)
    order_issues_df = build_order_issues(orders_quality_df)
    order_issues_df.show(20, truncate=False)

    print(f"Total de problemas encontrados: {order_issues_df.count()}")
    print("Problemas por tipo:")
    order_issues_df.groupBy("issue_type").count().orderBy(F.desc("count")).show(truncate=False)

    print("Rastreabilidade de pedidos que compartilham o mesmo id:")
    (
        orders_quality_df
        .filter(F.col("dq_duplicated_id"))
        .select("id", "client_id", "value_raw", *LINEAGE_COLUMNS)
        .orderBy("id", "client_id")
        .show(7, truncate=False)
    )

    print("=" * 80)
    print("SEÇÃO 1.2 - RELATÓRIO DE COMPRAS POR CLIENTE (TOP 20)")
    print("=" * 80)
    client_report_df, volume_threshold = build_client_report(orders_quality_df)
    print(f"Limite de volume de pedidos por cliente (Q3 + {OUTLIER_IQR_FACTOR} x IQR): {volume_threshold}")
    client_report_df.show(20, truncate=False)

    print("=" * 80)
    print("SEÇÃO 1.3 - TRANSFORMAÇÃO CONDICIONAL (CATEGORIA DE VALOR)")
    print("=" * 80)
    build_value_category_sample(orders_quality_df).show(20, truncate=False)

    # Transparência da regra literal: mostra quantos pedidos de cada categoria
    # estão fora da faixa válida (negativos em Low, acima de 99.99 em High).
    print("Distribuição das categorias na visão tratada:")
    (
        orders_quality_df
        .groupBy("value_category", "dq_invalid_range")
        .count()
        .orderBy("value_category", "dq_invalid_range")
        .show(truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
