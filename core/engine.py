from datetime import datetime

class DataQualityEngine:

    def __init__(self, adapter):
        self.adapter = adapter
        self.spark = adapter.spark

    def run(self, df, dataset_name):
        # 1. registra dataset se não existir
        self.adapter.ensure_dataset(dataset_name)

        # 2. registra colunas novas
        self.adapter.ensure_columns(dataset_name, df)

        # 3. carrega regras ativas
        rules_df = self.adapter.load_rules(dataset_name)

        if rules_df is None or rules_df.count() == 0:
            return {
                "dataset": dataset_name,
                "status": "NO_RULES"
            }

        # 4. aplica regras (placeholder simples)
        failed = rules_df.filter("is_active = true").count() == 0

        return {
            "dataset": dataset_name,
            "rules_applied": rules_df.count(),
            "status": "OK" if not failed else "FAILED"
        }
