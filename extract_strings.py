from __future__ import annotations

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
        for lang in ("python", "js", "c", "java", "go", "csharp")
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
    "cs": "csharp",
}

import codecs
import re

def is_string_literal(node):
    is_string = (node.type == "string_literal" or 
                node.type == "interpreted_string_literal" or 
                node.type == "string" or 
                node.type == "template_string" or 
                node.type == "raw_string_literal" or
                node.type == "verbatim_string_literal" or
                node.type == "interpolated_string_expression")
    if is_string:
        # print the children of the node
        # print(f"\nNode type: {node.type}, children types: {[child.type for child in node.children]}")
        # print(f"Node text: {node.text.decode('utf-8') if hasattr(node, 'text') else 'N/A'}")
        # print(f"Node child text: {[child.text.decode('utf-8') if hasattr(child, 'text') else 'N/A' for child in node.children]}")

        return True
    return False

def get_string_content(node, source_code):
    # Special handling for verbatim string literals in C#
    if node.type == "verbatim_string_literal":
        # Extract the content directly from the source code
        text = source_code[node.start_byte:node.end_byte]
        # Remove the @" prefix and " suffix
        if text.startswith('@"') and text.endswith('"'):
            return text[2:-1]
        return text

    # Special handling for interpolated string expressions in C#
    if node.type == "interpolated_string_expression":
        # Extract the content directly from the source code
        text = source_code[node.start_byte:node.end_byte]
        # Remove the $" prefix and " suffix
        if text.startswith('$"') and text.endswith('"'):
            return text[2:-1]
        return text

    # find the string_fragment or multiline_string_fragment
    for child in node.children:
        content_types = (
            "string_fragment", "multiline_string_fragment", "string_content", "raw_string_content",
            'raw_string_literal_content', 'interpreted_string_literal_content', 'string_literal_content')
        if child.type in content_types:
            return child.text.decode('utf-8') if hasattr(child, 'text') else ''

    # if there are only two children, the first is the starting and the second is the ending quote,
    # there is no content child
    if len(node.children) == 2:
        return ''

    if len(node.children) == 3:
        logger.debug("Node has 3 children, assuming the second is the content child: type=%s, text=%s",
                        node.type, node.text.decode('utf-8') if hasattr(node, 'text') else 'N/A')
        return node.children[1].text.decode('utf-8') if hasattr(node.children[1], 'text') else ''

    # for all other lengths, thake the content of all children except the first and last
    if len(node.children) > 3:
        content = []
        for child in node.children[1:-1]:
            if hasattr(child, 'text'):
                content.append(child.text.decode('utf-8'))
        logger.debug("Node has more than 3 children, concatenating content of all children except the first and last: type=%s, text=%s",
                        node.type, node.text.decode('utf-8') if hasattr(node, 'text') else 'N/A')
        return ''.join(content)


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
    is_src: bool = False
) -> List[str]:
    """Return *all* string literals found in *path*.

    Parameters
    ----------
    path      : file to analyse.
    language  : explicit language key (otherwise guessed from extension).
    dedupe    : if *True*, remove duplicates while keeping first‑occurrence order.
    """

    if not is_src:
        path = Path(path)
        src = Path(path).read_text("utf-8", errors="ignore")
        lang = (language or _EXTENSION_MAP.get(path.suffix.lstrip(".").lower()))
    else:
        src = path
        lang = language
        if lang is None:
            # raise an error if no language is specified
            raise ValueError("No language specified and no extension mapping found")

    if lang is None:
        logger.debug("Skipping %s: no language specified and no extension mapping found", path)
        return []
    else:
        logger.debug("Extracting strings from %s (%s)", path, lang)

    try:
        extractor = _EXTRACTORS[lang.lower()]
    except KeyError as exc:
        raise ValueError(f"No extractor registered for language '{lang}'") from exc

    result = extractor(src)
    return list(OrderedDict.fromkeys(result)) if dedupe else result


# ----------------------------------------------------------------------------
# Python – high fidelity via tokenize + ast.literal_eval
# ----------------------------------------------------------------------------


@extractor_for("python")
def extract_python_strings(src: str) -> List[str]:
    """Extract string literals from Python source code using tree-sitter.
    This function uses the tree-sitter parser to extract string literals from Python source code.
    It handles both single and double quoted strings, as well as multi-line strings.
    """

    parser = get_parser('python')

    # Parse the source code
    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node

    # Extract all string literals from the root node
    return extract_all_strings(src, root_node)


# ----------------------------------------------------------------------------
# JavaScript / TypeScript – template & classic literals
# ----------------------------------------------------------------------------

@extractor_for("js")
def extract_js_strings(src) -> List[str]:

    parser = get_parser('javascript')

    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node

    return extract_all_strings(src, root_node)


@extractor_for("c", "cpp")
def extract_c_strings(src: str) -> List[str]:

    parser = get_parser('cpp')

    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node

    return extract_all_strings(src, root_node)



@extractor_for("java")
def extract_java_strings(src: str)-> list[str]:


    parser = get_parser('java')

    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node


    return extract_all_strings(src, root_node)



@extractor_for("go")
def extract_go_strings(src: str) -> List[str]:

    parser = get_parser('go')

    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node

    return extract_all_strings(src, root_node)


@extractor_for("csharp")
def extract_csharp_strings(src: str) -> List[str]:

    parser = get_parser('csharp')

    tree = parser.parse(src.encode('utf-8'))
    root_node = tree.root_node

    return extract_all_strings(src, root_node)


if __name__ == "__main__":
    # Example usage
    path = "main.py"  # Replace with your file path
    strings = extract_strings(path, language="java", dedupe=True)
    print(strings)
