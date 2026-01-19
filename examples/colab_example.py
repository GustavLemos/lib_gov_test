#opcao 1
!pip install git+https://github.com/sua-org/LIB_GOV_TEST.git@main
#opcao 2
!git clone https://github.com/sua-org/LIB_GOV_TEST.git
!pip install -e LIB_GOV_TEST


from pyspark.sql import SparkSession
from gov_credit.core.engine import DataQualityEngine
from gov_credit.adapters.colab import ColabAdapter

# =========================================================
# Spark local (Colab)
# =========================================================
spark = SparkSession.builder \
    .appName("gov_credit_colab_example") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .getOrCreate()

# =========================================================
# Base path do Delta (ajuste se necessário)
# =========================================================
DQ_BASE_PATH = "/content/delta/dq"
DATASET_BASE_PATH = "/content/delta/data"

# =========================================================
# Adapter Colab
# =========================================================
adapter = ColabAdapter(
    spark=spark,
    dq_base_path=DQ_BASE_PATH
)

# =========================================================
# Engine de Qualidade
# =========================================================
dq_engine = DataQualityEngine(adapter)

# =========================================================
# Dataset de exemplo
# =========================================================
data = [
    ("1", "Ana", 42),
    ("2", "Carlos", 19),
    ("3", "Bruna", None)
]

columns = ["cliente_id", "nome", "idade"]

df = spark.createDataFrame(data, columns)

# =========================================================
# EXECUÇÃO DA GOVERNANÇA
# =========================================================
dq_engine.run(
    df=df,
    dataset_name="clientes_credito"
)

# =========================================================
# WRITE EM DELTA POR PATH
# =========================================================
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{DATASET_BASE_PATH}/clientes_credito")

print(" Pipeline executado com governança no Colab")
