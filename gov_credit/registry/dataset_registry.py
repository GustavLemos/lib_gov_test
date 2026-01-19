
import uuid
from pyspark.sql.functions import current_timestamp

class DatasetRegistry:
    def __init__(self, spark):
        self.spark = spark

    def get_or_create_dataset(self, dataset_name):
        df = (
            self.spark.table("dq.datasets")
            .filter(f"dataset_name = '{dataset_name}' AND active = true")
        )

        if df.count() > 0:
            return df.first()["dataset_id"], False

        dataset_id = str(uuid.uuid4())
        row = [(dataset_id, dataset_name, dataset_name.split(".")[0], True)]
        (
            self.spark.createDataFrame(
                row, ["dataset_id", "dataset_name", "layer", "active"]
            )
            .withColumn("created_at", current_timestamp())
            .write.mode("append")
            .saveAsTable("dq.datasets")
        )

        return dataset_id, True

    def detect_new_columns(self, dataset_id, df):
        existing = (
            self.spark.table("dq.columns")
            .filter(f"dataset_id = '{dataset_id}'")
            .select("column_name")
            .collect()
        )
        existing_cols = {r.column_name for r in existing}

        new_cols = {}
        for field in df.schema.fields:
            if field.name not in existing_cols:
                new_cols[field.name] = {
                    "data_type": str(field.dataType),
                    "nullable": field.nullable
                }
        return new_cols

    def register_new_columns(self, dataset_id, new_columns):
        if not new_columns:
            return

        rows = [
            (dataset_id, col, meta["data_type"], meta["nullable"])
            for col, meta in new_columns.items()
        ]

        (
            self.spark.createDataFrame(
                rows, ["dataset_id", "column_name", "data_type", "nullable"]
            )
            .withColumn("created_at", current_timestamp())
            .write.mode("append")
            .saveAsTable("dq.columns")
        )
