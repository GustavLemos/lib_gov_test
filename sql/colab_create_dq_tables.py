# ==========================================================

# gov_credit - Data Quality Governance Tables (Colab)

# ==========================================================

# Este script cria as tabelas Delta usadas pela biblioteca

# gov_credit em ambientes sem metastore (ex: Colab).

#

# Ele deve ser executado UMA VEZ antes do uso da biblioteca.

# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import (
StructType, StructField,
StringType, BooleanType, TimestampType, LongType
)

spark = SparkSession.builder 
.appName("gov_credit_bootstrap") 
.getOrCreate()

# ----------------------------------------------------------

# AJUSTE ESTE PATH CONFORME SEU AMBIENTE

# Pode ser:

# - /content/delta/dq

# - s3://bucket/dq

# - gs://bucket/dq

# ----------------------------------------------------------

DQ_BASE_PATH = "/content/delta/dq"

# ==========================================================

# DATASETS

# ==========================================================

datasets_schema = StructType([
StructField("dataset_id", StringType(), False),
StructField("dataset_name", StringType(), False),
StructField("dataset_path", StringType(), True),
StructField("format", StringType(), True),
StructField("owner", StringType(), True),
StructField("domain", StringType(), True),
StructField("created_at", TimestampType(), True),
StructField("updated_at", TimestampType(), True),
StructField("active", BooleanType(), True),
])

spark.createDataFrame([], datasets_schema) 
.write.format("delta") 
.mode("ignore") 
.save(f"{DQ_BASE_PATH}/datasets")

# ==========================================================

# COLUMNS

# ==========================================================

columns_schema = StructType([
StructField("dataset_id", StringType(), False),
StructField("column_name", StringType(), False),
StructField("data_type", StringType(), True),
StructField("nullable", BooleanType(), True),
StructField("created_at", TimestampType(), True),
StructField("updated_at", TimestampType(), True),
StructField("active", BooleanType(), True),
])

spark.createDataFrame([], columns_schema) 
.write.format("delta") 
.mode("ignore") 
.save(f"{DQ_BASE_PATH}/columns")

# ==========================================================

# RULES

# ==========================================================

rules_schema = StructType([
StructField("rule_id", StringType(), False),
StructField("dataset_id", StringType(), False),
StructField("column_name", StringType(), True),
StructField("rule_type", StringType(), False),
StructField("rule_definition", StringType(), True),
StructField("severity", StringType(), True),
StructField("active", BooleanType(), True),
StructField("created_at", TimestampType(), True),
StructField("updated_at", TimestampType(), True),
])

spark.createDataFrame([], rules_schema) 
.write.format("delta") 
.mode("ignore") 
.save(f"{DQ_BASE_PATH}/rules")

# ==========================================================

# RESULTS

# ==========================================================

results_schema = StructType([
StructField("execution_id", StringType(), False),
StructField("dataset_id", StringType(), False),
StructField("column_name", StringType(), True),
StructField("rule_id", StringType(), True),
StructField("rule_type", StringType(), True),
StructField("status", StringType(), True),
StructField("failed_rows", LongType(), True),
StructField("total_rows", LongType(), True),
StructField("execution_time", TimestampType(), True),
])

spark.createDataFrame([], results_schema) 
.write.format("delta") 
.mode("ignore") 
.save(f"{DQ_BASE_PATH}/results")

print("✅ Estruturas de Data Quality criadas com sucesso em:", DQ_BASE_PATH)
