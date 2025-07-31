import os

import pandas as pd
from langchain_ollama import ChatOllama
from tqdm import tqdm

from src.config import DATABASE_PATH, logger
import duckdb
from typing import List, Dict, Any, Optional, Literal, Tuple

from langchain_core.language_models import BaseLLM
from pydantic import BaseModel, Field


class ModelResult(BaseModel):
    """Always use this tool to structure your response to the user."""
    result: List[Tuple[str, str]] = Field(description="List of tuples (semantic_type, physical_type) for each column.")

ModeOption = Literal['find_semantic_types', 'find_string_misuse']


SYSTEM_PROMPT = """
You will be given multiple columns and their sample values from a database. For each column, determine:

1. Its **semantic type**, based on its name and sample values.  
2. Its **ideal physical type** for storing it in a DBMS giving the best performance (`string`, `integer`, `float`, `boolean`, `timestamp`).

These are the semantic types you can choose from:

1. **Name** – names of entities or persons, titles, labels  
2. **DateTime** – dates, times, timestamps  
3. **Numeric** – numeric values such as counts, prices, scores, sizes  
4. **Boolean** – true/false flags, typically with prefixes like `is*`, `has*`, `enabled`  
5. **Category** – discrete class-like values such as type, role, status, tag  
6. **FullText** – free-form or long-form text like descriptions, messages, notes  
7. **Identifier** – unique IDs such as uuid, hash, code, token, version  
8. **Contact** – emails, phone numbers, fax, mobile  
9. **Location** – cities, countries, regions, postal addresses, zip codes  
10. **URL** – web links, slugs, image URLs, icon paths  
11. **Test** – non-semantic placeholders like “test”, “col”, dummy data

Do not use any other types

Return a list of tuples (semantic_type, physical_type). Example:
Column 1: `users.name` with values `["Alice", "Bob"]` 
Column 2: `users.created_at` with values `["2023-01-01", "2023-01-02"]`
Result:
[("Name", "string"), ("DateTime", "timestamp")]
"""


CATEGORIES = [
    "Name", "DateTime", "Numeric", "Boolean", "Category",
    "FullText", "Identifier", "Email", "PhoneNumber", "Location", "URL"
]

def get_data() -> List[List[Dict[str, Any]]]:
    """
    Retrieve column data from the database and organize it into batches.

    Returns:
        List of batches, where each batch contains dictionaries with table_name, column_name, and values.
    """
    # con = duckdb.connect(DATABASE_PATH, read_only=True)
    con = duckdb.connect('/Users/paul/workspace/SqlPile/data/schemapile_29_07.duckdb', read_only=True)
    con.execute("""
                  CREATE TEMP VIEW column_usages_unnested AS
                  (
                  SELECT *, unnest(column_ids) AS column_id
                  FROM column_usages
                  )
                  """)
    query = """
            SELECT list(DISTINCT column_id) as column_ids,
                   table_name,
                   column_name,
                   list(DISTINCT value)[:10] as "values"
            FROM (
                FROM column_usages_unnested 
                -- WHERE column_id NOT IN (SELECT column_id FROM '/Users/paul/workspace/SqlPile/src/data_analysis/semantic_types.csv')
            )
                JOIN values_often USING (column_id)
                JOIN columns
            on (values_often.column_id = columns.id)
                JOIN TABLES on tables.id = columns.table_id
                JOIN QUERIES on queries.id = query_id
            GROUP BY table_name, column_name
            ORDER BY table_name
            """

    result = con.execute(query).fetchall()

    # Create batches of 20 rows


    batches = []
    MAX_CHARACTERS = 100
    for i in range(0, len(result), BATCH_SIZE):
        batch = result[i:i + BATCH_SIZE]
        json_data = []
        for row in batch:
            column_ids, table_name, column_name, values = row

            # only add values until there a maximum of 150 characters
            values_reduced = []
            total_characters = 0

            while total_characters < MAX_CHARACTERS and values:

                remaining_characters = MAX_CHARACTERS - total_characters
                value = values.pop(0)
                values_reduced.append(value[:remaining_characters])
                total_characters += len(value)

            json_data.append({
                "column_ids": column_ids,
                "table_name": table_name,
                "column_name": column_name,
                "values": values_reduced
            })
        batches.append(json_data)

    return batches


def setup_model() -> BaseLLM:
    base = ChatOllama(
        model=MODEL,
        keep_alive=-1,
        timeout=60,
    )
    model =  base.with_structured_output(ModelResult)

    # Add retry with keyword args:
    retryable = model.with_retry(
        stop_after_attempt=3,
        wait_exponential_jitter=True
    )

    return retryable



def get_messages_for_data(batch: List[Dict[str, Any]]) -> List[Dict[str, str]]:

    columns_info = []
    for i, column_data in enumerate(batch):
        column_info = f"""Column {i + 1}:
    Table: {column_data['table_name']}
    Column: {column_data['column_name']}
    Sample Values: {column_data['values'][:10]}
    """
        columns_info.append(column_info)

    prompt = "Determine the semantic type and the ideal physical type for each of the following columns:\n\n" + "\n".join(columns_info)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt}
    ]

    return messages

def run_prompt_on_batch(batch: List[Dict[str, Any]], model: BaseLLM) -> Tuple[List[Optional[str]], List[Optional[str]]]:
    try:
        # Format the prompt with information about all columns in the batch


        messages = get_messages_for_data(batch)
        response = model.invoke(messages, think=False)
        semantic_types, physical_types = zip(*response.result)

        # check if all semantic types are in the expected categories
        if not all(st in CATEGORIES for st in semantic_types):
            messages = messages + [
                {"role": "assistant", "content": f"Invalid result detected: {set(semantic_types) - set(CATEGORIES)}. Please ensure all types are one of the following: {CATEGORIES}"}
            ]
            response = model.invoke(messages, think=False)
            semantic_types, physical_types = zip(*response.result)

        # If the response is not a list, log an error and return None for each column
        if not all(isinstance(st, str) for st in semantic_types):
            logger.error(f"Invalid response format: {semantic_types}. Expected a list of strings.")
            return [None] * len(batch), [None] * len(batch)

        # Ensure we have the right number of types
        if len(semantic_types) != len(batch) or len(physical_types) != len(batch):
            logger.error(f"Mismatch in number of results: expected {len(batch)}, got {len(semantic_types)} semantic types and {len(physical_types)} physical types.")
            return [None] * len(batch), [None] * len(batch)

        # Log the results
        for i, (column_data, semantic_type, physical_type) in enumerate(zip(batch, semantic_types, physical_types)):
            logger.info(
                f"Determined result for {column_data['table_name']}.{column_data['column_name']}: Values {column_data['values'][:5]}, Semantic Type: {semantic_type}, Ideal Physical Type: {physical_type}")

        return semantic_types, physical_types

    except Exception as e:
        logger.error(f"Error determining result for batch: {str(e)}")
        return [None] * len(batch), [None] * len(batch)

def process_batches(batches: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:

    model = setup_model()
    results = []

    for batch_idx, batch in tqdm(enumerate(batches), total=len(batches), desc="Processing batches"):
        logger.info(f"Processing batch {batch_idx+1}/{len(batches)} ({len(batch)} columns)")

        # Process the entire batch in a single request using structured output
        sematic_types, physical_types = run_prompt_on_batch(batch, model)

        # Add semantic types to the column data
        for column_data, semantic_result, physical_type in zip(batch, sematic_types, physical_types):
            column_result = column_data.copy()
            column_result['semantic_type'] = semantic_result
            column_result['physical_type'] = physical_type
            results.append(column_result)

        save_results(results)

    return results

def save_results(results: List[Dict[str, Any]]) -> None:

    # Convert results to DataFrame
    new_df = pd.DataFrame(results)
    new_df = new_df[['column_ids', 'table_name', 'column_name', 'values', 'semantic_type', 'physical_type']].copy()
    new_df = new_df.explode('column_ids')
    new_df['column_ids'] = new_df['column_ids'].astype(str)
    new_df.rename(columns={'column_ids': 'column_id'}, inplace=True)

    output_file = f"semantic_types.csv"

    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        existing_df['column_id'] = existing_df['column_id'].astype(str)
        new_df = new_df[~new_df['column_id'].isin(existing_df['column_id'])]
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df

    final_df.to_csv(output_file, index=False)



BATCH_SIZE = 5
MODEL = 'qwen3:8b'

if __name__ == "__main__":
    logger.info("Starting semantic type determination")
    batches = get_data()
    logger.info(f"Retrieved {len(batches)} batches with a total of {sum(len(batch) for batch in batches)} columns")

    results = process_batches(batches)
    save_results(results)

    logger.info("Semantic type determination completed")
