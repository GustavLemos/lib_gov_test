
from gov_credit.registry.dataset_registry import DatasetRegistry
from gov_credit.rules.rule_loader import RuleLoader

class DataQualityEngine:
    def __init__(self, spark):
        self.spark = spark
        self.registry = DatasetRegistry(spark)
        self.rule_loader = RuleLoader(spark)

    def run(self, df, dataset_name: str):
        dataset_id, created = self.registry.get_or_create_dataset(dataset_name)

        new_columns = self.registry.detect_new_columns(dataset_id, df)
        if new_columns:
            self.registry.register_new_columns(dataset_id, new_columns)

        rules = self.rule_loader.load(dataset_name)

        if not rules:
            return {
                "status": "NO_RULES",
                "new_columns": list(new_columns.keys())
            }

        errors = []
        for rule in rules:
            col = rule["column_name"]
            if rule["rule_type"] == "not_null":
                failed = df.filter(f"{col} IS NULL").count()
                if failed > 0:
                    errors.append({
                        "column": col,
                        "rule": "not_null",
                        "failed_rows": failed
                    })

        if errors:
            return {"status": "FAILED", "errors": errors}

        return {"status": "SUCCESS"}
