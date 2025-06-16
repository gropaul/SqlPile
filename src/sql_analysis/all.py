from src.sql_analysis.analyze_queries_in_db import analyze_queries_in_db
from src.sql_analysis.get_schemas_from_create_query import get_schemas_from_create_query
from src.sql_analysis.load_queries_to_database import load_queries_to_database
from src.sql_analysis.load_schemapile_json_to_ddb import load_schemapile_json_to_database


def all():
    load_schemapile_json_to_database(ask=False)
    load_queries_to_database(ask=False)
    get_schemas_from_create_query()
    analyze_queries_in_db()



if __name__ == "__main__":
    all()