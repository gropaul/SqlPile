from docs.gen.sec_dataset_description import generate_dataset_description
from docs.gen.sec_logical_analysis import generate_logical_analysis
from docs.gen.sec_semantic_analysis import generate_semantic_analysis
from docs.gen.sec_storage_analysis import generate_storage_analysis


def run_all():
    generate_dataset_description()
    generate_logical_analysis()
    generate_semantic_analysis()
    generate_storage_analysis()


if __name__ == "__main__":
    run_all()