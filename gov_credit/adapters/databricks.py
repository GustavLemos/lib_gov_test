from pyspark.sql.functions import lit, current_timestamp

class DatabricksAdapter:

    def __init__(self, spark):
        self.spark = spark

    def ensure_dataset(self, dataset_name):
        self.spark.sql(f"""
            INSERT INTO dq.datasets
            SELECT '{dataset_name}', current_timestamp()
            WHERE NOT EXISTS (
                SELECT 1 FROM dq.datasets WHERE dataset_name = '{dataset_name}'
            )
        """)

    def ensure_columns(self, dataset_name, df):
        cols = [(c, t) for c, t in df.dtypes]

        for col, dtype in cols:
            self.spark.sql(f"""
                INSERT INTO dq.columns
                SELECT '{dataset_name}', '{col}', '{dtype}', current_timestamp()
                WHERE NOT EXISTS (
                    SELECT 1 FROM dq.columns
                    WHERE dataset_name = '{dataset_name}'
                    AND column_name = '{col}'
                )
            """)

    def load_rules(self, dataset_name):
        return (
            self.spark.table("dq.rules")
            .filter(f"dataset_name = '{dataset_name}' AND is_active = true")
        )
