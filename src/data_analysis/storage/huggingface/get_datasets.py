from huggingface_hub import HfApi
import yaml
import duckdb

from src.data_analysis.storage.huggingface.models import ParseResult, DataFile, Config, Columns
from src.data_analysis.storage.huggingface.store import store_results


def save_result(con: duckdb.DuckDBPyConnection):
    pass


def parse_card_data(id: str, card_data_str: str) -> ParseResult | None:
    if not card_data_str:
        print("Empty card data string.")
        return None
    # parse the YAML-like string in card_data_str to extract configs and columns
    card_data = yaml.safe_load(card_data_str)
    configs_parsed = []
    columns_parsed = []

    download_size = 0
    dataset_size = 0

    size_categories = card_data.get('size_categories', '')
    if isinstance(size_categories, list):
        size_categories = ','.join(size_categories)

    splis = []

    if 'dataset_info' in card_data:
        dataset_info = card_data['dataset_info']
        if isinstance(dataset_info, list):
            dataset_info = dataset_info[0]

        download_size = dataset_info.get('download_size', 0)
        dataset_size = dataset_info.get('dataset_size', 0.0)

        if 'features' in dataset_info:
            features = dataset_info['features']
            for feature in features:
                name = feature.get('name', '')
                dtype = feature.get('dtype', '')
                columns_parsed.append(Columns(name, dtype))

        if 'splits' in dataset_info:
            splits = dataset_info['splits']
            for split in splits:
                name = split.get('name', '')
                num_bytes = split.get('num_bytes', 0.0)
                num_examples = split.get('num_examples', 0)
                splis.append((name, num_bytes, num_examples))

        else:
            print("No features found in dataset_info.")
            print(card_data)
    else:
        print("No features found in dataset_info.")
        print(card_data)

    if 'configs' in card_data:
        configs = card_data['configs']
        for config in configs:
            config_name = config.get('name', 'default')
            data_files = config.get('data_files', [])

            if not isinstance(data_files, list):
                data_files = [data_files]

            data_files_parsed = []
            for data_file in data_files:
                if isinstance(data_file, str):  # if it is a string, make it a DataFile with default split
                    data_files_parsed.append(DataFile(id, 'default', data_file))
                    continue

                split = data_file.get('split', 'default')
                path = data_file.get('path', '')
                data_files_parsed.append(DataFile(id, split, path))

            configs_parsed.append(Config(config_name, data_files_parsed))

        return ParseResult(id, configs_parsed, columns_parsed, size_categories, download_size, dataset_size, splis)

    else:
        return None


url_params = "modality:tabular,format:parquet"
api = HfApi()
dataset = api.list_datasets(
    filter=url_params,
    sort="downloads",
    full=True,
    gated=False,
    direction=-1,
    limit=100,
    token=False
)

results = []
for i, d in enumerate(dataset):
    print(f"Dataset {i + 1}: {d.id}")


    # list the files in the dataset
    # files = api.list_repo_files(d.id, repo_type="dataset")
    # get the card data
    if not d.cardData:
        print("No card data found.")
        continue
    card_data = d.cardData.to_yaml()

    parse_result = parse_card_data(d.id, card_data)
    if parse_result is not None:
        results.append(parse_result)

store_results(results, mode='append')



