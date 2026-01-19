
class RuleLoader:
    def __init__(self, spark):
        self.spark = spark

    def load(self, dataset_name):
        return (
            self.spark.table("dq.rules")
            .filter(f"dataset_name = '{dataset_name}' AND active = true")
            .collect()
        )
