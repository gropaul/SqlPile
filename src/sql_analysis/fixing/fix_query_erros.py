# Install necessary packages:
# pip install langchain langchain-ollama langchain-openai
import getpass
import os
from typing import Optional, List, Literal, Dict

import duckdb
from langchain_openai import ChatOpenAI

from langchain_core.language_models import BaseLLM
from langchain_ollama.llms import OllamaLLM
from langchain.schema import HumanMessage, SystemMessage
import time

from sqloxide import parse_sql

from src.config import DATABASE_PATH, logger
from src.sql_analysis.execution.prepare_sql_for_execution import prepare_sql_statically
from src.sql_analysis.load_schemapile_json_to_ddb import ERROR_TABLE_NAME

SYSTEM_PROMPT = """
Construct a valid sql query from this code. If there are parameters (e.g. $1, %s, :param, etc.), replace them with example values.
Only return the query. If there are multiple queries, separate them by newlines.
"""
if not os.environ.get("OPENAI_API_KEY"):
    os.environ["OPENAI_API_KEY"] = getpass.getpass("Enter your OpenAI API key: ")

ModelName = Literal['ollama:qwen3:1.7b', 'ollama:qwen3:4b', 'ollama:qwen3:8b', 'openai:gpt-3.5-turbo', 'openai:gpt-4.1-nano']

MODELS_ESCALATION_ORDER: List[ModelName] = [
    # 'ollama:qwen3:1.7b',
    'ollama:qwen3:4b',
    # 'openai:gpt-3.5-turbo',
    # 'openai:gpt-4.1-nano',
]

# make the user confirm if there is a openai model in the escalation order as this will incur costs
if any(model.startswith('openai:') for model in MODELS_ESCALATION_ORDER):
    confirm = input("You have selected OpenAI models in the escalation order. This will incur costs. Do you want to continue? (yes/no): ")
    if confirm.lower() != 'yes':
        MODELS_ESCALATION_ORDER = [model for model in MODELS_ESCALATION_ORDER if not model.startswith('openai:')]
        logger.warning("OpenAI models removed from escalation order.")


MODELS: Dict[ModelName, BaseLLM] = {
    'ollama:qwen3:1.7b': OllamaLLM(model="qwen3:1.7b"),
    'ollama:qwen3:4b': OllamaLLM(model="qwen3:4b"),
    'ollama:qwen3:8b': OllamaLLM(model="qwen3:8b"),
    'openai:gpt-3.5-turbo': ChatOpenAI(model="gpt-3.5-turbo"),
    'openai:gpt-4.1-nano': ChatOpenAI(model="gpt-4.1-nano"),
}

MODELS_ARGS: Dict[str, Dict] = {
    'ollama:qwen3:1.7b': {'think': False},
    'ollama:qwen3:4b': {'think': False},
    'ollama:qwen3:8b': {'think': False},
    'openai:gpt-3.5-turbo': {},
    'openai:gpt-4.1-nano': {},
}

INITIAL_MESSAGES = [SystemMessage(content=SYSTEM_PROMPT)]


def format_seconds(seconds: float) -> str:
    """Format seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        return f"{seconds / 60:.2f} minutes"
    else:
        return f"{seconds / 3600:.2f} hours"


def can_parse_query(query: str) -> bool:
    try:
        parse_sql(sql=query, dialect='generic')
        return True
    except Exception as e:
        logger.error(f"Failed to parse query: Error: {e}")
        return False


class FixResult:
    def __init__(self, code: str, fixed_query: Optional[str], model_name: Optional[ModelName]):
        self.code = code
        self.fixed_query = fixed_query
        self.model_name = model_name

    def was_successful(self) -> bool:
        return self.fixed_query is not None

    def __str__(self):
        return f"FixResult(fixed_query={self.fixed_query}, model_name={self.model_name})"


def try_to_fix_query(extracted_sql: str, code: str) -> FixResult:
    if not code:
        print("No code found for this query, skipping.")
        return FixResult(code=code, fixed_query=None, model_name=None)

    logger.info(f"Fixing query with length {len(code)} characters.")
    messages_with_code = INITIAL_MESSAGES + [HumanMessage(content=code)]

    for model_name in MODELS_ESCALATION_ORDER:
        model = MODELS[model_name]
        args = MODELS_ARGS[model_name]
        try:
            response = model.invoke(messages_with_code, **args)
            # if the response is an object and has the 'content' attribute, use that
            if hasattr(response, 'content'):
                response = response.content
            fixed_query = prepare_sql_statically(response)
            if can_parse_query(fixed_query):
                logger.info(f"Successfully fixed query with model {model_name}.")
                return FixResult(code=code, fixed_query=fixed_query, model_name=model_name)
            else:
                logger.warning(f"Model {model_name} failed to produce a valid query")
                logger.warning(f"Original query: {extracted_sql}")
                logger.warning(f"Response from model: {response}")
        except Exception as e:
            print(e)
            pass
    return FixResult(code=code, fixed_query=None, model_name=None)


def fix_queries():
    con = duckdb.connect(DATABASE_PATH, read_only=False)

    clean_queries = []

    code_context_window_start = 10
    code_context_window_end = 100
    # get the queries to fix
    queries = con.execute(f"""
        SELECT 
            sql, 
            text_context[text_context_offset-{code_context_window_start}:-text_context_offset+{code_context_window_end}] as context,
            queries.rowid as rowid
        FROM {ERROR_TABLE_NAME} 
        JOIN queries ON queries.id = queries_error.query_id
        WHERE 'Query parsing' in error_message AND
             len(queries.text_context) != 0
        GROUP BY ALL
        ORDER BY rowid
        LIMIT 50
    """).fetchall()

    # benchmark how long it takes to invoke the LLM

    start_time = time.time()
    for sql, code, _row_id in queries:
        fixed_sql = try_to_fix_query(extracted_sql=sql, code=code)
        if fixed_sql.was_successful():
            clean_queries.append(fixed_sql)
        else:
            logger.warning(f"Failed to fix query: {sql}")

    end_time = time.time()

    print(f"Fixed {len(clean_queries)} queries.")
    for i, query in enumerate(clean_queries):
        print(f"{i + 1}: Model: {query.model_name}, Code length: {len(query.code)}")
        print(" - Original code:")
        print(query.code)
        print(" - Fixed query:")
        print(query.fixed_query)
        print("-" * 80)

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    # print how long it would take to get 10_000 queries fixed
    extrapolated_time = (end_time - start_time) * (10_000 / len(clean_queries))
    print(f"Extrapolated time for 10,000 queries: {format_seconds(extrapolated_time)}")

    # Track successful fixes and total code lengths per model
    model_success_count = {}
    code_length_if_success = {}

    for query in clean_queries:
        if query.was_successful() and query.model_name:
            model_success_count[query.model_name] = model_success_count.get(query.model_name, 0) + 1
            code_length_if_success[query.model_name] = code_length_if_success.get(query.model_name, 0) + len(query.code)

    print("Model success count and average code length:")
    for model_name in model_success_count:
        count = model_success_count[model_name]
        total_length = code_length_if_success.get(model_name, 0)
        average_length = total_length / count if count > 0 else 0
        print(f"{model_name}: {count} successful fixes, average code length = {average_length:.2f}")


if __name__ == "__main__":
    fix_queries()
