from __future__ import annotations

from pyjsparser import parse as js_parse
import ast
import codecs
import importlib
import re
import tokenize
from collections import OrderedDict
from pathlib import Path
from typing import Callable, List, Dict, Tuple

from config import logger
from tree_sitter_language_pack import get_parser

_EXTRACTORS: Dict[str, Callable[[str | Path], List[str]]] = {}


def extractor_for(*aliases: str):
    """Decorator to register a function under one or more *language* keys."""

    def register(func: Callable[[str | Path], List[str]]):
        for alias in aliases:
            _EXTRACTORS[alias.lower()] = func
        return func

    return register


# ----------------------------------------------------------------------------
# Public façade
# ----------------------------------------------------------------------------

__all__ = [
    "extract_strings",
    *[
        f"extract_{lang}_strings"
        for lang in ("python", "js", "c", "java", "go")
    ],
]


_EXTENSION_MAP = {
    # Guess‑by‑extension table (no dot, lower‑case)
    "py": "python",
    "js": "js",
    "jsx": "js",
    "ts": "js",
    "tsx": "js",
    "c": "c",
    "h": "c",
    "cpp": "cpp",
    "cc": "cpp",
    "cxx": "cpp",
    "hpp": "cpp",
    "hh": "cpp",
    "java": "java",
    "kt": "java",
    "go": "go",
}

import codecs
import re

def is_string_literal(node):
    is_string = node.type == "string_literal" or node.type == "interpreted_string_literal" or node.type == "string" or node.type == "template_string" or node.type == "raw_string_literal"
    if is_string:
        # print the children of the node
        # print(f"\nNode type: {node.type}, children types: {[child.type for child in node.children]}")
        # print(f"Node text: {node.text.decode('utf-8') if hasattr(node, 'text') else 'N/A'}")
        # print(f"Node child text: {[child.text.decode('utf-8') if hasattr(child, 'text') else 'N/A' for child in node.children]}")

        return True
    return False

def get_string_content(node, source_code):
    # find the string_fragment or multiline_string_fragment
    for child in node.children:
        content_types = (
            "string_fragment", "multiline_string_fragment", "string_content", "raw_string_content",
            'raw_string_literal_content', 'interpreted_string_literal_content')
        if child.type in content_types:
            return child.text.decode('utf-8') if hasattr(child, 'text') else ''

    error_message = f"Node {node.type} does not have a string content child. Node text: {node.text.decode('utf-8') if hasattr(node, 'text') else 'N/A'};\n"
    for (child_index, child) in enumerate(node.children):
        error_message += f"child_index={child_index} type={child.type} content={child.text.decode('utf-8') if hasattr(child, 'text') else 'N/A'};\n"

    start_byte = max(0, node.start_byte - 20)
    end_byte = min(len(source_code), node.end_byte + 20)
    error_message += f"Source code snippet: {source_code[start_byte:end_byte]!r}\n"

    raise ValueError(error_message.strip())


def extract_joined_string(node, source_code):
    if is_string_literal(node):
        return [get_string_content(node, source_code)]

    if node.type == "binary_expression":
        op_node = next((c for c in node.children if c.type == '+'), None)
        if not op_node:
            return []
        left = extract_joined_string(node.children[0], source_code)
        right = extract_joined_string(node.children[2], source_code)
        return left + right

    return []


def extract_all_strings(source_code: str, root_node):
    collected = []

    def visit(node, parent=None):
        if node.type == "binary_expression":
            joined = extract_joined_string(node, source_code)
            if joined:
                collected.append("".join(joined))
                return  # skip inner children, already processed

        elif is_string_literal(node):
            if not (parent and parent.type == "binary_expression"):
                string = get_string_content(node, source_code)
                collected.append(string)

        for child in node.children:
            visit(child, node)

    visit(root_node)
    return collected


def extract_strings(
    path: str | Path,
    language: str | None = None,
    *,
    dedupe: bool = False,
) -> List[str]:
    """Return *all* string literals found in *path*.

    Parameters
    ----------
    path      : file to analyse.
    language  : explicit language key (otherwise guessed from extension).
    dedupe    : if *True*, remove duplicates while keeping first‑occurrence order.
    """

    path = Path(path)
    lang = (language or _EXTENSION_MAP.get(path.suffix.lstrip(".").lower()))
    if lang is None:
        logger.info("Skipping %s: no language specified and no extension mapping found", path)
        return []

    try:
        extractor = _EXTRACTORS[lang.lower()]
    except KeyError as exc:
        raise ValueError(f"No extractor registered for language '{lang}'") from exc

    result = extractor(path)
    return list(OrderedDict.fromkeys(result)) if dedupe else result


# ----------------------------------------------------------------------------
# Python – high fidelity via tokenize + ast.literal_eval
# ----------------------------------------------------------------------------


@extractor_for("python")
def extract_python_strings(path: str | Path) -> List[str]:
    strings: list[str] = []
    with Path(path).expanduser().open("rb") as fh:
        for tok_type, tok_text, *_ in tokenize.tokenize(fh.readline):
            if tok_type == tokenize.STRING:
                try:
                    value = ast.literal_eval(tok_text)
                except Exception:
                    value = tok_text.strip("\"'")
                if isinstance(value, str):
                    strings.append(value)
    return strings


# ----------------------------------------------------------------------------
# JavaScript / TypeScript – template & classic literals
# ----------------------------------------------------------------------------

@extractor_for("js")
def extract_js_strings(path: str | Path) -> List[str]:
    src = Path(path).read_text("utf-8", errors="ignore")

    parser = get_parser('javascript')

    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node

    return extract_all_strings(src, root_node)


@extractor_for("c", "cpp")
def extract_c_strings(path: str | Path) -> List[str]:
    """Extract literal strings from a C/C++ source file.

    Uses Tree‑sitter if it is available and configured.
    """
    src = Path(path).read_text("utf-8", errors="ignore")

    parser = get_parser('cpp')

    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node

    return extract_all_strings(src, root_node)



@extractor_for("java")
def extract_java_strings(path: str | Path)-> list[str]:

    src = Path(path).read_text("utf-8", errors="ignore")

    parser = get_parser('java')

    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node


    return extract_all_strings(src, root_node)



@extractor_for("go")
def extract_go_strings(path: str | Path) -> List[str]:
    src = Path(path).read_text("utf-8", errors="ignore")

    parser = get_parser('go')

    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node

    return extract_all_strings(src, root_node)


if __name__ == "__main__":
    # Example usage
    path = "main.py"  # Replace with your file path
    strings = extract_strings(path, language="java", dedupe=True)
    print(strings)