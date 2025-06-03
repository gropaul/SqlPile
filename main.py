import json
from typing import Optional

from analyse_repo import analyse_repo
from config import logger
from extract_sql import RepoAnalysisResult


def read_data():
    """Read the JSON data from schemapile-perm.json file."""
    # read the json schemapile-perm.json file
    with open('.data/schemapile-perm.json', 'r') as file:
        data = json.load(file)
    return data


def main():
    """Main function to process repositories."""
    data = read_data()

    total_queries = 0

    for item in data:
        value = data[item]
        # Get the repository URL
        url = value['INFO']['URL']

        # Analyze the repository and clone it if it exists
        result: Optional[RepoAnalysisResult] = analyse_repo(url)

        if result is not None:
            total_queries += len(result.queries)
            # save the result to a file
            result.save()

        if total_queries > 50:
            logger.info(f"Total queries extracted: {total_queries}. Stopping further processing.")
            exit(0)


if __name__ == "__main__":
    main()
