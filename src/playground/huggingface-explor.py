from huggingface_hub import HfApi

TABULAR_EXTS = (".csv", ".parquet", ".sqlite", ".db", ".arrow", ".feather")

api = HfApi()

res = api.list_datasets(
    limit=3,
    full=True,
    filter=[
        'modality:tabular',
        'format:parquet',
        # 'format:csv',
    ],
    sort='downloads',

)
for r in res:
    print(r)

"""
DatasetInfo(
    id='ebeaulac/siriccam-sq-400px-16frag-6-25-valid-fragments', 
    author='ebeaulac', sha='6947abcc52b903fd4539b030cf2235ffa13ac196', 
    created_at=datetime.datetime(2022, 10, 10, 4, 25, 24, tzinfo=datetime.timezone.utc), 
    last_modified=datetime.datetime(2022, 10, 10, 4, 25, 29, tzinfo=datetime.timezone.utc), 
    private=False, 
    gated=False, 
    disabled=False, 
    downloads=3, 
    downloads_all_time=None, 
    likes=0, 
    paperswithcode_id=None, 
    tags=['size_categories:n<1K', 'format:parquet', 'modality:tabular', 'library:datasets', 'library:pandas', 'library:mlcroissant', 'library:polars', 'region:us'], 
    trending_score=0, 
    card_data=None, 
    siblings=None, 
    xet_enabled=None
)


DatasetInfo(
id='lavita/medical-qa-shared-task-v1-toy', 
author='lavita', sha='a1cbb5ce2b697b2377f49cad89ea6387a7efd3fa', 
created_at=datetime.datetime(2023, 7, 20, 0, 28, 51, tzinfo=datetime.timezone.utc), 
last_modified=datetime.datetime(2023, 7, 20, 0, 29, 6, tzinfo=datetime.timezone.utc), 
private=False, gated=False, disabled=False, downloads=909106, downloads_all_time=None, 
likes=21, paperswithcode_id=None, 
tags=['size_categories:n<1K', 'format:parquet', 'modality:tabular', 'modality:text', 'library:datasets', 'library:pandas', 'library:mlcroissant', 'library:polars', 'region:us'], 
trending_score=None, 
card_data={'annotations_creators': None, 'language_creators': None, 'language': None, 'license': None, 'multilinguality': None, 'size_categories': None, 'source_datasets': None, 'task_categories': None, 'task_ids': None, 'paperswithcode_id': None, 'pretty_name': None, 'config_names': None, 'train_eval_index': None, 'dataset_info': {'features': [{'name': 'id', 'dtype': 'int64'}, {'name': 'ending0', 'dtype': 'string'}, {'name': 'ending1', 'dtype': 'string'}, {'name': 'ending2', 'dtype': 'string'}, {'name': 'ending3', 'dtype': 'string'}, {'name': 'ending4', 'dtype': 'string'}, {'name': 'label', 'dtype': 'int64'}, {'name': 'sent1', 'dtype': 'string'}, {'name': 'sent2', 'dtype': 'string'}, {'name': 'startphrase', 'dtype': 'string'}], 'splits': [{'name': 'train', 'num_bytes': 52480.01886421694, 'num_examples': 32}, {'name': 'dev', 'num_bytes': 52490.64150943396, 'num_examples': 32}], 'download_size': 89680, 'dataset_size': 104970.6603736509}}, siblings=None, xet_enabled=None)

"""