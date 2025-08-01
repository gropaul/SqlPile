import os

import pandas as pd
from langchain_ollama import ChatOllama
from tqdm import tqdm

from src.config import DATABASE_PATH, logger
import duckdb
from typing import List, Dict, Any, Optional

from langchain_core.language_models import BaseLLM
from pydantic import BaseModel, Field


class SemanticTypes(BaseModel):
    """Always use this tool to structure your response to the user."""
    types: List[str] = Field(description="List of semantic types for each column")


SYSTEM_PROMPT = """
Based on the table name, column name, and sample values, determine the semantic type of each column.
Every column must fit one of the types:

1. **Name** – names of entities and persons, titles, labels
2. **DateTime** – dates, timestamps, time-related fields  
3. **Numeric** – amounts, counts, prices, scores, sizes  
4. **Boolean** – yes/no flags, is*/has*, enabled states  
5. **Category** – type, role, status, label, class, tag  
6. **FullText** – descriptions, messages, summaries, notes, usually long text
7. **Identifier** – id, uuid, code, hash, token, version  
8. **Contact** – emails, phone, fax, mobile 
9. **Location** – city, country, region, address, zip  
10. **URL** – link, url, image path, icon, slug
11. **Test** – columns that are not semantic, e.g. "test", "col"

You will be given multiple columns at once.
Return *only* the list of semantic types in the same order as the columns were provided.
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
    con = duckdb.connect(DATABASE_PATH, read_only=True)
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
                WHERE column_id NOT IN (SELECT column_id FROM '/Users/paul/workspace/SqlPile/src/data_analysis/semantic_types.csv')
            )
                JOIN values_often USING (column_id)
                JOIN columns
            on (values_often.column_id = columns.id)
                JOIN TABLES on tables.id = columns.table_id
                JOIN QUERIES on queries.id = query_id
            GROUP BY table_name, column_name
            ORDER BY table_name, column_name
            """

    result = con.execute(query).fetchall()

    # Create batches of 20 rows


    batches = []

    for i in range(0, len(result), BATCH_SIZE):
        batch = result[i:i + BATCH_SIZE]
        json_data = []
        for row in batch:
            column_ids, table_name, column_name, values = row

            # only add values until there a maximum of 150 characters
            values_reduced = []
            total_characters = 0

            MAX_WIDTH = 100
            while total_characters < MAX_WIDTH and values:

                remaining_characters = MAX_WIDTH - total_characters
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
    model =  base.with_structured_output(SemanticTypes)

    # Add retry with keyword args:
    retryable = model.with_retry(
        stop_after_attempt=3,
        wait_exponential_jitter=True
    )

    return retryable




def determine_semantic_types_batch(batch: List[Dict[str, Any]], model: BaseLLM) -> List[Optional[str]]:
    try:
        # Format the prompt with information about all columns in the batch
        columns_info = []
        for i, column_data in enumerate(batch):
            column_info = f"""Column {i+1}:
Table: {column_data['table_name']}
Column: {column_data['column_name']}
Sample Values: {column_data['values'][:10]}
"""
            columns_info.append(column_info)

        prompt = "Determine the semantic type for each of the following columns:\n\n" + "\n".join(columns_info)
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ]
        response = model.invoke(messages, think=False)
        semantic_types = response.types

        # check if all semantic types are in the expected categories
        if not all(st in CATEGORIES for st in semantic_types):
            messages = messages + [
                {"role": "assistant", "content": f"Invalid semantic types detected: {set(semantic_types) - set(CATEGORIES)}. Please ensure all types are one of the following: {CATEGORIES}"}
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
            return [None] * len(batch)

        # Log the results
        for i, (column_data, semantic_type) in enumerate(zip(batch, semantic_types)):
            logger.info(
                f"Determined semantic type for {column_data['table_name']}.{column_data['column_name']}: {semantic_type}")

        return semantic_types

    except Exception as e:
        logger.error(f"Error determining semantic types for batch: {str(e)}")
        return [None] * len(batch)

def process_batches(batches: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:

    model = setup_model()
    results = []

    for batch_idx, batch in tqdm(enumerate(batches), total=len(batches), desc="Processing batches"):
        logger.info(f"Processing batch {batch_idx+1}/{len(batches)} ({len(batch)} columns)")

        # Process the entire batch in a single request using structured output
        semantic_types = determine_semantic_types_batch(batch, model)

        # Add semantic types to the column data
        for column_data, semantic_type in zip(batch, semantic_types):
            column_result = column_data.copy()
            column_result['semantic_type'] = semantic_type
            results.append(column_result)

        save_results(results)

    return results

def save_results(results: List[Dict[str, Any]], output_file: str = "semantic_types.csv"):
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


BATCH_SIZE = 5
MODEL = 'qwen3:8b'

if __name__ == "__main__":
    logger.info("Starting semantic type determination")
    batches = get_data()
    logger.info(f"Retrieved {len(batches)} batches with a total of {sum(len(batch) for batch in batches)} columns")

    results = process_batches(batches)
    save_results(results)

    logger.info("Semantic type determination completed")
