%pip install git+https://github.com/sua-org/LIB_GOV_TEST.git@main
dbutils.library.restartPython()

from pyspark.sql import SparkSession
from gov_credit.core.engine import DataQualityEngine
from gov_credit.adapters.databricks import DatabricksAdapter

# =========================================================
# Spark (Databricks já fornece)
# =========================================================
spark = SparkSession.builder.getOrCreate()

# =========================================================
# Adapter Databricks
# =========================================================
adapter = DatabricksAdapter(
    spark=spark,
    dq_schema="dq"  # schema onde estão datasets, columns, rules
)

# =========================================================
# Engine de Qualidade
# =========================================================
dq_engine = DataQualityEngine(adapter)

# =========================================================
# Dataset de exemplo
# =========================================================
data = [
    ("1", "João", 35),
    ("2", "Maria", None),
    ("3", "Pedro", 28)
]

columns = ["cliente_id", "nome", "idade"]

df = spark.createDataFrame(data, columns)

# =========================================================
# EXECUÇÃO DA GOVERNANÇA
# (sempre antes do write)
# =========================================================
dq_engine.run(
    df=df,
    dataset_name="clientes_credito"
)

# =========================================================
# WRITE NORMAL DO PIPELINE
# =========================================================
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.clientes_credito")

print(" Pipeline executado com governança Databricks")
