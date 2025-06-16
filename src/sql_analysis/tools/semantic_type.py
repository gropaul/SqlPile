from typing import Optional

_NAME_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ID", ("id", "uuid", "guid")),
    (
        "AMOUNT",
        (
            "amount",
            "price",
            "cost",
            "value",
            "budget",
            "fee",
            "salary",
            "revenue",
            "expense",
        ),
    ),
    ("COUNT", ("count", "quantity", "number", "total")),
    (
        "DATE",
        (
            "date",
            "time",
            "timestamp",
            "created",
            "updated",
            "month",
            "year",
            "day",
            "_at",
        ),
    ),
    ("NAME", ("name", "title")),
    ("PASSWORD", ("password", "passcode", "secret", "token", "secret")),
    ("EMAIL", ("email",)),
    ("URL", ("url", "link")),
    ("STATUS", ("status", "state")),
    ("ADDRESS", ("address", "location", "city", "country", "zip", "postal", "street")),
    ("CATEGORY", ("category", "type", "kind")),
    ("FULL_TEXT", ("text", "description", "content", "body", "summary", "note", "comment")),
)

_NUMERIC_TYPES = {"Int", "Float"}

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _contains_any(lower_name: str, needles: tuple[str, ...]) -> bool:
    """Return *True* if *any* of *needles* is a substring of *lower_name*."""

    return any(word in lower_name for word in needles)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_column_semantic_type(column_name: str, category: str) -> Optional[str]:
    name_lc = column_name.lower()

    # --- direct type‑based classifications ---------------------------------
    if category == "Boolean":
        return "BOOLEAN"
    if category == "DateTime":
        return "TIMESTAMP"

    # --- name‑based heuristics (first matching rule wins) -------------------
    for semantic, keywords in _NAME_RULES:
        if _contains_any(name_lc, keywords):
            # Guard against nonsensical mappings like "price" being VARCHAR.
            if semantic in {"AMOUNT", "COUNT"} and category not in _NUMERIC_TYPES:
                continue
            return semantic

    # --- fallback classifications ------------------------------------------
    if category == "Binary":
        return "BINARY"
    if category == "JSON":
        return "JSON"

    # Nothing matched.
    return None


__all__ = [
    "get_column_semantic_type",
]
