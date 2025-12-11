from typing import List, Literal, Optional

SYSTEM_PROMPT = """
Based on the table name, column name, and sample values, determine the semantic type of each column. 
Base your decision rather on the values than the column name.
Every column must fit one of the types:

{{TYPES_LIST}}

You will be given multiple columns at once.
Return *only* the list of semantic types in the same order as the columns were provided.
"""


class SemanticType:
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    def __repr__(self):
        return f"SemanticType(name={self.name}, description={self.description})"

SemanticTypeColumnName = Literal['semantic_type_llm', 'semantic_type_llm_subtype']

class SemanticTypeRunConfig:
    def __init__(self, semantic_types: List[SemanticType], target_column: SemanticTypeColumnName, filter_semantic_type: Optional[str], allow_other: bool = True):
        self.semantic_types = semantic_types
        self.target_column = target_column
        self.filter_semantic_type = filter_semantic_type
        if allow_other:
            self.semantic_types.append(SemanticType("Other", "when none of the other types fit"))

    def __repr__(self):
        return f"SemanticTypeRunConfig(semantic_types={self.semantic_types}, filter_null_column={self.target_column}, filter_semantic_type={self.filter_semantic_type})"

BASE_SEMANTIC_TYPES = [
    SemanticType("Numeric", "dates, times, timestamps, amounts, counts, prices, scores, sizes, values with unit"),
    SemanticType("Category", "type, role, status, label, class, tag, yes/no flags, is*/has*"),
    SemanticType("FullText", "natural language like descriptions, messages, summaries, notes, comments"),
    SemanticType("Identifier", "id, uuid, code, hash, token, version, link, url, slug, emails, phone numbers, ..."),
    SemanticType("Entity", "entities or names of entities like person (names), titles, labels, cities, countries, addresses"),
    SemanticType("Structured", "like JSON, CSV or simple lists e.g. 'a,b,c'"),
    SemanticType("Test", "column with content/name that is for testing, e.g. 'test', 'col', 'val1'"),
]

BASE_CONFIG = SemanticTypeRunConfig(
    semantic_types=BASE_SEMANTIC_TYPES,
    target_column="semantic_type_llm",
    filter_semantic_type=None
)

# subtypes of Identifier
ID_SEMANTIC_TYPES = [
    SemanticType("Path", "web addresses, links, URIs, file paths"),
    SemanticType("Semantic", "Ids that are specific to a certain domain, e.g. product codes, SKU, ISBN, personal ids like emails, phone numbers"),
    SemanticType("Generic", "generic database keys, UUIDs, auto-incremented IDs, hashes"),
]

ID_CONFIG = SemanticTypeRunConfig(
    semantic_types=ID_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Identifier",
    allow_other=False
)

# subtypes of Numeric
NUMERIC_SEMANTIC_TYPES = [
    SemanticType("DateTime", "dates, times, or datetime, related"),
]

NUMERIC_CONFIG = SemanticTypeRunConfig(
    semantic_types=NUMERIC_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Numeric"
)


# subtypes of Category
CATEGORY_SEMANTIC_TYPES = [
    SemanticType("Boolean", "binary yes/no or true/false indicators such as is_active or has_value"),
]

CATEGORY_CONFIG = SemanticTypeRunConfig(
    semantic_types=CATEGORY_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Category"
)


# subtypes of FullText
FULLTEXT_SEMANTIC_TYPES = [
    SemanticType("Unformatted", "other plain text without special markup"),
    SemanticType("Formatted", "text with markup such as Markdown or HTML"),
]

FULLTEXT_CONFIG = SemanticTypeRunConfig(
    semantic_types=FULLTEXT_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="FullText",
    allow_other=False
)


# subtypes of Entity
ENTITY_SEMANTIC_TYPES = [
    SemanticType("Person", "names of people, including all variations like first name, last name, full name, initials"),
    SemanticType("Organization", "companies, institutions, agencies, or other groups"),
    SemanticType("Location", "geographical locations such as cities, countries, addresses, landmarks"),
]

ENTITY_CONFIG = SemanticTypeRunConfig(
    semantic_types=ENTITY_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Entity"
)


# subtypes of Structured
Structured_SEMANTIC_TYPES = [
    SemanticType("JSON", "JSON-like structured data"),
    SemanticType("CSV", "comma- or delimiter-separated text values"),
    SemanticType("List", "simple lists such as 'a,b,c' or '[1,2,3]'"),
]

STRUCTURED_CONFIG = SemanticTypeRunConfig(
    semantic_types=Structured_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Structured"
)

SUB_CONFIGS = [
    ID_CONFIG,
    NUMERIC_CONFIG,
    CATEGORY_CONFIG,
    FULLTEXT_CONFIG,
    ENTITY_CONFIG,
    STRUCTURED_CONFIG,
]


def get_prompt(types: List[SemanticType]) -> str:
    types_list = "\n".join([f"- {t.name}: {t.description}" for t in types])
    return SYSTEM_PROMPT.replace("{{TYPES_LIST}}", types_list)