import os

import pandas as pd
from langchain_ollama import ChatOllama
from tqdm import tqdm

from src.config import DATABASE_PATH, logger, get_con
import duckdb
from typing import List, Dict, Any, Optional, Tuple, TypedDict, Literal

from langchain_core.language_models import BaseLLM
from pydantic import BaseModel, Field

from src.data_analysis.semantic_type.models import get_prompt, SemanticType, BASE_SEMANTIC_TYPES, SemanticTypeRunConfig, \
    SemanticTypeColumnName, BASE_CONFIG, ID_CONFIG, SUB_CONFIGS

DataSetType = Literal['kaggle', 'sqlpile']

class OutputSchema(BaseModel):
    """Always use this tool to structure your response to the user."""
    types: List[str] = Field(description="List of semantic types for each column")


DataRow = Tuple[List[int], str, str, List[str]]


class DataRowJson(TypedDict):
    column_ids: List[int]
    table_name: str
    column_name: str
    values: List[str]


def get_sql_pile_data(con: duckdb.DuckDBPyConnection, filter_column_null: str | None, filter_semantic_type: str | None) -> List[DataRow]:
    """
    Retrieve column data from the database and organize it into batches.

    Returns:
        List of batches, where each batch contains dictionaries with table_name, column_name, and values.
    """

    filter_null_clause = "" if filter_column_null is None else f" AND {filter_column_null} IS NULL"
    filter_semantic_type_clause = "" if filter_semantic_type is None else f" AND columns.semantic_type_llm = '{filter_semantic_type}'"
    query = f"""
            WITH ids_to_process AS (SELECT DISTINCT column_id
                                    FROM column_values 
                                )
            SELECT list(DISTINCT column_id) as column_ids,
                   table_name,
                   column_name,
                   list(DISTINCT value)[:20] as "values"
            FROM ids_to_process
            JOIN column_values USING (column_id)
            JOIN columns ON column_values.column_id = columns.id
            JOIN tables ON tables.id = columns.table_id
            JOIN table_values_count as tvc ON tvc.table_id = tables.id
            JOIN repos ON tables.repo_id = repos.id
            WHERE 
                ( 
                    tvc.count > 2000 -- the column will be used for statistics, e.g. kaggle columns
                    OR column_id IN (SELECT column_id FROM column_usages_unnested) -- the column is used in queries
                )  
                AND length(value) > 0
                {filter_null_clause}
                {filter_semantic_type_clause}
            GROUP BY tables.repo_id, table_name, column_name
            ORDER BY tables.repo_id DESC, table_name, column_name
            """
    print(query)
    result = con.execute(query).fetchall()
    return result


def batch_data(data: List[DataRow]) -> List[List[DataRowJson]]:
    # Create batches of 20 rows

    batches: List[List[DataRowJson]] = []
    for i in range(0, len(data), BATCH_SIZE):
        input_batch = data[i:i + BATCH_SIZE]
        json_data = []
        for row in input_batch:
            column_ids, table_name, column_name, values = row

            # if values is None or len(values) == 0:
            if not values or len(values) == 0:
                logger.warning(f"No values found for {table_name}.{column_name}. Skipping this column.")
                continue

            # only add values until there a maximum of 150 characters
            values_reduced = []
            total_characters = 0

            MAX_WIDTH = 250
            while total_characters < MAX_WIDTH and values:
                remaining_characters = MAX_WIDTH - total_characters
                value = values.pop(0)
                if not value:
                    logger.error(f"Empty value found for {table_name}.{column_name}. Skipping this value. Remaining values: {values}")
                    continue
                values_reduced.append(value[:remaining_characters])
                total_characters += len(value)

            row: DataRowJson = {
                'column_ids': column_ids,
                'table_name': table_name,
                'column_name': column_name,
                'values': values_reduced
            }
            json_data.append(row)

        if len(json_data) == 0:
            logger.warning(f"All columns in batch starting at index {i} were skipped due to lack of values.")

        batches.append(json_data)

    return batches


def setup_model() -> BaseLLM:
    base = ChatOllama(
        model=MODEL,
        keep_alive=-1,
        timeout=60,
    )
    model = base.with_structured_output(OutputSchema)

    # Add retry with keyword args:
    retryable = model.with_retry(
        stop_after_attempt=3,
        wait_exponential_jitter=True
    )

    return retryable


def determine_semantic_types_batch(batch: List[DataRowJson], model: BaseLLM, input_semantic_types: List[SemanticType]) -> List[Optional[str]]:

    system_prompt = get_prompt(input_semantic_types)

    try:
        # Format the prompt with information about all columns in the batch
        columns_info = []
        for i, column_data in enumerate(batch):
            column_info = f"""Column {i + 1}:
Table: {column_data['table_name']}
Column: {column_data['column_name']}
Sample Values: {column_data['values'][:10]}
"""
            columns_info.append(column_info)

        prompt = "Determine the semantic type for each of the following columns:\n\n" + "\n".join(columns_info)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]
        response = model.invoke(messages, think=False)
        response_semantic_types = response.types

        # check if all semantic types are in the expected categories
        allowed_types = [st.name for st in input_semantic_types]
        if not all(st in allowed_types for st in response_semantic_types):
            messages = messages + [
                {"role": "assistant",
                 "content": f"Invalid semantic types detected: {set(response_semantic_types) - set(allowed_types)}. Please ensure all types are one of the following: {allowed_types}"}
            ]
            response = model.invoke(messages, think=False)
        semantic_types = response.types

        # If the response is not a list, log an error and return None for each column
        if not all(isinstance(st, str) for st in semantic_types):
            logger.error(f"Invalid response format: {semantic_types}. Expected a list of strings.")
            return [None] * len(batch)

        # Ensure we have the right number of types
        if len(semantic_types) != len(batch):
            logger.error(f"Mismatch in number of semantic types: expected {len(batch)}, got {len(semantic_types)}")
            print(f"Prompt was: {prompt}")
            print(f"The batch was: {batch}")

            return [None] * len(batch)

        # Log the results
        for i, (column_data, semantic_type) in enumerate(zip(batch, semantic_types)):
            logger.info(
                f"Determined semantic type for {column_data['table_name']}.{column_data['column_name']}: {semantic_type}")

        return semantic_types

    except Exception as e:
        logger.error(f"Error determining semantic types for batch: {str(e)}")
        return [None] * len(batch)


def process_batches(batches: List[List[DataRowJson]], output_file: str, semantic_types_input: List[SemanticType], target_column: SemanticTypeColumnName) -> List[Dict[str, Any]]:
    model = setup_model()
    results = []

    for batch_idx, batch in tqdm(enumerate(batches), total=len(batches), desc="Processing batches"):
        logger.info(f"Processing batch {batch_idx + 1}/{len(batches)} ({len(batch)} columns)")

        # Process the entire batch in a single request using structured output
        semantic_types = determine_semantic_types_batch(batch, model, semantic_types_input)

        # Add semantic types to the column data
        for column_data, semantic_type in zip(batch, semantic_types):
            column_result = column_data.copy()
            column_result['semantic_type'] = semantic_type
            results.append(column_result)

        save_results(results, output_file=output_file, target_column=target_column)

    return results


def save_results(results: List[Dict[str, Any]], target_column: SemanticTypeColumnName, output_file: str = "semantic_types_sqlpile.csv"):
    # Convert results to DataFrame
    new_df = pd.DataFrame(results)
    new_df = new_df[['column_ids', 'table_name', 'column_name', 'semantic_type']]
    new_df = new_df.explode('column_ids')
    new_df['column_ids'] = new_df['column_ids'].astype(str)
    new_df.rename(columns={'column_ids': 'column_id'}, inplace=True)

    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        existing_df['column_id'] = existing_df['column_id'].astype(str)
        new_df = new_df[~new_df['column_id'].isin(existing_df['column_id'])]
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df

    final_df.to_csv(output_file, index=False)

    copy_csv_to_database(new_df, target_column=target_column)




BATCH_SIZE = 5
MODEL = 'qwen3:8b'

def clear_columns():
    con = duckdb.connect(DATABASE_PATH)
    con.execute("ALTER TABLE columns DROP COLUMN IF EXISTS semantic_type_llm;")
    con.execute("ALTER TABLE columns DROP COLUMN IF EXISTS semantic_type_llm_subtype;")

    add_columns()

def add_columns():
    con = duckdb.connect(DATABASE_PATH)
    con.execute("ALTER TABLE columns ADD COLUMN IF NOT EXISTS semantic_type_llm VARCHAR;")
    con.execute("ALTER TABLE columns ADD COLUMN IF NOT EXISTS semantic_type_llm_subtype VARCHAR;")

def copy_csv_to_database(df: pd.DataFrame, target_column: SemanticTypeColumnName):
    con = duckdb.connect(DATABASE_PATH)

    for _, row in df.iterrows():
        column_id = row['column_id']
        semantic_type = row['semantic_type']
        con.execute(f"""
            UPDATE columns
            SET {target_column} = ?
            WHERE id = ?
        """, (semantic_type, column_id))


def run_semantic_type_analysis(output_file: str, config: SemanticTypeRunConfig):

    add_columns()

    # reset the output file
    if os.path.exists(output_file):
        os.remove(output_file)

    logger.info("Starting semantic type determination")
    con = get_con(read_only=True)
    data = get_sql_pile_data(con, filter_column_null=config.target_column, filter_semantic_type=config.filter_semantic_type)
    con.close()

    batches = batch_data(data)
    logger.info(f"Retrieved {len(batches)} batches with a total of {sum(len(batch) for batch in batches)} columns")

    results = process_batches(batches, output_file=output_file, semantic_types_input=config.semantic_types, target_column=config.target_column)
    logger.info("Semantic type determination completed")


if __name__ == "__main__":
    output_file = "semantic_types_sqlpile.csv"
    # run_semantic_type_analysis(output_file, BASE_CONFIG)

    for sub_config in SUB_CONFIGS:
        logger.info(f"Running semantic type analysis for config: {sub_config.filter_semantic_type}")
        output_file = f"semantic_types_sqlpile_{sub_config.filter_semantic_type.lower()}.csv"
        run_semantic_type_analysis(output_file, sub_config)



