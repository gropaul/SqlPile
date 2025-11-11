from typing import List, Literal

SYSTEM_PROMPT = """
Based on the table name, column name, and sample values, determine the semantic type of each column.
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
    def __init__(self, semantic_types: List[SemanticType], target_column: SemanticTypeColumnName, filter_semantic_type: str | None):
        self.semantic_types = semantic_types
        self.target_column = target_column
        self.filter_semantic_type = filter_semantic_type
        self.semantic_types.append(SemanticType("Other", "when none of the other types fit"))

    def __repr__(self):
        return f"SemanticTypeRunConfig(semantic_types={self.semantic_types}, filter_null_column={self.target_column}, filter_semantic_type={self.filter_semantic_type})"

BASE_SEMANTIC_TYPES = [
    SemanticType("Numeric", "dates, times, timestamps, amounts, counts, prices, scores, sizes, values with unit"),
    SemanticType("Category", "type, role, status, label, class, tag, yes/no flags, is*/has*"),
    SemanticType("FullText", "descriptions, messages, summaries, notes, comments"),
    SemanticType("Identifier", "id, uuid, code, hash, token, version, link, url, slug, emails, phone numbers"),
    SemanticType("Entity", "names of entities and persons, titles, labels, cities, countries, addresses"),
    SemanticType("Semistructured", "like JSON, CSV or simple lists e.g. 'a,b,c'"),
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
    SemanticType("DomainSpecificID", "Ids that are specific to a certain domain, e.g. product codes, SKU, ISBN, vehicle registration numbers, so the id has human readable parts"),
    SemanticType("DatabaseKey", "generic database keys, UUIDs, auto-incremented IDs, hashes"),
]

ID_CONFIG = SemanticTypeRunConfig(
    semantic_types=ID_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Identifier"
)

# subtypes of Numeric
NUMERIC_SEMANTIC_TYPES = [
    SemanticType("Amount", "monetary or measurable values such as prices, weights, or sizes with units"),
    SemanticType("Timestamp", "dates, times, or datetimes"),
]

NUMERIC_CONFIG = SemanticTypeRunConfig(
    semantic_types=NUMERIC_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Numeric"
)


# subtypes of Category
CATEGORY_SEMANTIC_TYPES = [
    SemanticType("Boolean", "binary yes/no or true/false indicators such as is_active or has_value"),
    SemanticType("Category", "discrete labels or types such as status, role, or type"),
]

CATEGORY_CONFIG = SemanticTypeRunConfig(
    semantic_types=CATEGORY_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Category"
)


# subtypes of FullText
FULLTEXT_SEMANTIC_TYPES = [
    SemanticType("ShortText", "short phrases or titles providing descriptive content"),
    SemanticType("LongText", "longer free-form text such as descriptions, comments, or notes"),
    SemanticType("FormattedText", "text with markup such as Markdown or HTML"),
]

FULLTEXT_CONFIG = SemanticTypeRunConfig(
    semantic_types=FULLTEXT_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="FullText"
)


# subtypes of Entity
ENTITY_SEMANTIC_TYPES = [
    SemanticType("PersonName", "names of individuals"),
    SemanticType("Organization", "companies, institutions, or brands"),
    SemanticType("Location", "geographic entities such as cities, regions, or countries"),
]

ENTITY_CONFIG = SemanticTypeRunConfig(
    semantic_types=ENTITY_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Entity"
)


# subtypes of Semistructured
SEMISTRUCTURED_SEMANTIC_TYPES = [
    SemanticType("JSON", "JSON-like structured data"),
    SemanticType("CSV", "comma- or delimiter-separated text values"),
    SemanticType("List", "simple lists such as 'a,b,c' or '[1,2,3]'"),
]

SEMISTRUCTURED_CONFIG = SemanticTypeRunConfig(
    semantic_types=SEMISTRUCTURED_SEMANTIC_TYPES,
    target_column="semantic_type_llm_subtype",
    filter_semantic_type="Semistructured"
)

SUB_CONFIGS = [
    ID_CONFIG,
    NUMERIC_CONFIG,
    CATEGORY_CONFIG,
    FULLTEXT_CONFIG,
    ENTITY_CONFIG,
    SEMISTRUCTURED_CONFIG,
]


def get_prompt(types: List[SemanticType]) -> str:
    types_list = "\n".join([f"- {t.name}: {t.description}" for t in types])
    return SYSTEM_PROMPT.replace("{{TYPES_LIST}}", types_list)