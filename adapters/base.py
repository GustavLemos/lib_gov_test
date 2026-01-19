class BaseAdapter:

    def ensure_dataset(self, dataset_name):
        raise NotImplementedError

    def ensure_columns(self, dataset_name, df):
        raise NotImplementedError

    def load_rules(self, dataset_name):
        raise NotImplementedError
