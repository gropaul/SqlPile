from typing import List


class DataFile:
    def __init__(self, id: str, split: str, path: str):
        self.split = split

        # if path is a list take the first element
        if isinstance(path, list):
            path = path[0]

        self.path = path

        # if the path ends with a * at .parquet at the end
        if path.endswith("*"):
            self.path += ".parquet"

        self.path = "hf://datasets/" + id + "/" + self.path


class ParquetFile:

    def __init__(self, id: str, config: str, split: str, path: str, size_bytes: int):

        self.id = id
        self.split = split
        self.path = path
        self.config = config
        self.size_bytes = size_bytes


class Columns:
    def __init__(self, name: str, dtype: str):
        self.name = name
        self.dtype = dtype

    def __repr__(self):
        return f"Columns(name={self.name}, dtype={self.dtype})"

class DatasetInfo:
    def __init__(self, features: List[Columns]):
        self.features = features

    def __repr__(self):
        return f"DatasetInfo(features={self.features})"

class Splits:
    def __init__(self, name: str, num_bytes: float, num_examples: int):
        self.name = name
        self.num_bytes = num_bytes
        self.num_examples = num_examples

    def __repr__(self):
        return f"Splits(name={self.name}, num_bytes={self.num_bytes}, num_examples={self.num_examples})"



class Config:
    def __init__(self, name: str, data_files: List[DataFile]):
        self.name = name
        self.data_files = data_files

    def __repr__(self):
        return f"Config(name={self.name}, data_files={self.data_files})"


class ParseResult:
    def __init__(self, id: str, configs: List[Config], columns: List[Columns],
                 size_categories: str, download_size: int, dataset_size: float, license: str, splits: List[Splits], parquet_files: List[ParquetFile]):
        self.id = id
        self.configs = configs
        self.columns = columns
        self.size_categories = size_categories
        self.download_size = download_size
        self.dataset_size = dataset_size
        self.splits = splits
        self.license = license
        self.parquet_files = parquet_files

    def __repr__(self):
        return f"ParseResult(id={self.id}, configs={self.configs}, columns={self.columns})"