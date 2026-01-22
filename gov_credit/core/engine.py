from pyspark.sql.functions import col
from datetime import datetime


class DataQualityException(Exception):
    """Exceção lançada quando regras de qualidade falham"""
    pass


class DataQualityEngine:

    def __init__(self, adapter, fail_fast=True):
        self.adapter = adapter
        self.spark = adapter.spark
        self.fail_fast = fail_fast

    def run(self, df, dataset_name):
        # 1. Registra dataset se não existir
        self.adapter.ensure_dataset(dataset_name)

        # 2. Registra colunas novas
        self.adapter.ensure_columns(dataset_name, df)

        # 3. Carrega regras ativas
        rules_df = self.adapter.load_rules(dataset_name)

        if rules_df is None or rules_df.count() == 0:
            return {
                "dataset": dataset_name,
                "rules_applied": 0,
                "status": "NO_RULES",
                "violations": []
            }

        rules = rules_df.collect()
        violations = []

        # 4. Aplica regras
        for rule in rules:
            if rule.rule_type == "not_null":
                invalid_count = df.filter(col(rule.column_name).isNull()).count()

                if invalid_count > 0:
                    violations.append({
                        "column": rule.column_name,
                        "rule": "not_null",
                        "invalid_rows": invalid_count
                    })

            # extensível: range, regex, allowed_values etc

        # 5. Fail-fast: quebra notebook/pipeline se houver violação
        if violations and self.fail_fast:
            raise DataQualityException(
                f"Data Quality FAILED for dataset '{dataset_name}': {violations}"
            )

        status = "FAILED" if violations else "OK"

        return {
            "dataset": dataset_name,
            "rules_applied": len(rules),
            "status": status,
            "violations": violations
        }
