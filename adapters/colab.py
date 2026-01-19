from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

def create_spark():
    return (
        SparkSession.builder
        .appName("gov_credit")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

class ColabAdapter:

    def __init__(self, base_path="/content/delta/dq"):
        self.base_path = base_path
        self.spark = create_spark()

    def ensure_dataset(self, dataset_name):
        path = f"{self.base_path}/datasets"
        try:
            df = self.spark.read.format("delta").load(path)
        except:
            df = self.spark.createDataFrame([], "dataset_name STRING, created_at TIMESTAMP")

        if df.filter(f"dataset_name = '{dataset_name}'").count() == 0:
            new = self.spark.createDataFrame(
                [(dataset_name,)], ["dataset_name"]
            ).withColumn("created_at", current_timestamp())

            new.write.format("delta").mode("append").save(path)

    def ensure_columns(self, dataset_name, df):
        path = f"{self.base_path}/columns"
        try:
            cols_df = self.spark.read.format("delta").load(path)
        except:
            cols_df = self.spark.createDataFrame(
                [], "dataset_name STRING, column_name STRING, data_type STRING, created_at TIMESTAMP"
            )

        for col, dtype in df.dtypes:
            exists = cols_df.filter(
                f"dataset_name='{dataset_name}' AND column_name='{col}'"
            ).count()

            if exists == 0:
                new = self.spark.createDataFrame(
                    [(dataset_name, col, dtype)],
                    ["dataset_name", "column_name", "data_type"]
                ).withColumn("created_at", current_timestamp())

                new.write.format("delta").mode("append").save(path)

    def load_rules(self, dataset_name):
        path = f"{self.base_path}/rules"
        try:
            return (
                self.spark.read.format("delta").load(path)
                .filter(f"dataset_name = '{dataset_name}' AND is_active = true")
            )
        except:
            return None
