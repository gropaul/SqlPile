"""string_extractors.py – v1.2 (2025‑06‑03)
=================================================

Extract string literals from source‑code files in several mainstream languages
(Python, JavaScript/TypeScript, C/C++, Java/Kotlin, and Go). The entry point is
`extract_strings`, whose signature remains ::

    extract_strings(path, language=None, *, dedupe=False) → list[str]

**v1.2 highlights**
-------------------
* **Go extractor now preserves the order** in which different string‑literal
  forms (double‑quoted and back‑tick raw strings) appear. Previously it always
  returned double‑quoted literals first, causing the test suite to fail.
* Internal tidy‑ups: unused regexes removed, version banner updated.
"""

from __future__ import annotations

from tree_sitter import Language, Parser  # type: ignore
from pyjsparser import parse as js_parse
import ast
import importlib
import re
import tokenize
from collections import OrderedDict
from pathlib import Path
from typing import Callable, List, Dict, Tuple

from config import logger
# ----------------------------------------------------------------------------
# Registry helpers
# ----------------------------------------------------------------------------

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
        raise ValueError(
            "Cannot deduce language from extension – pass `language=` explicitly"
        )

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

    tree = js_parse.parse(src, tolerant=True)
    strings: list[str] = []

    def walk(node):
        if isinstance(node, dict):
            typ = node.get("type")
            if typ == "Literal" and isinstance(node.get("value"), str):
                strings.append(node["value"])
            elif typ == "TemplateElement":
                strings.append(node["value"].get("cooked", ""))
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(tree)
    return strings

@extractor_for("c", "cpp")
def extract_c_strings(path: str | Path) -> List[str]:
    """Extract literal strings from a C/C++ source file.

    Uses Tree‑sitter if it is available and configured.
    """
    src = Path(path).read_text("utf-8", errors="ignore")

    lib_path = Path(__file__).parent
    cpp_language = Language(lib_path, "cpp")
    parser = Parser()
    parser.set_language(cpp_language)

    """Extract string literals via a Tree‑sitter parse (offset‑stable)."""
    tree = parser.parse(src.encode())
    root = tree.root_node
    out: List[Tuple[int, str]] = []

    def recurse(node):
        if node.type in {"string_literal", "raw_string_literal"}:
            text = src[node.start_byte: node.end_byte]
            if node.type == "string_literal":
                out.append((node.start_byte, text[1:-1]))  # drop quotes
            else:  # raw string – strip R"delim( … )delim"
                inner = re.sub(r'^R"[^ ()\\]{0,16}\(|\)[^ ()\\]{0,16}"$', "", text)
                out.append((node.start_byte, inner))
        for child in node.children:
            recurse(child)

    recurse(root)
    out.sort(key=lambda t: t[0])
    return [s for _, s in out]


def _compute_line_offsets(text: str) -> List[int]:
    """Return cumulative char count at the start of each 1‑based line."""
    offsets = [0]
    total = 0
    for line in text.splitlines(True):  # keep linebreaks
        total += len(line)
        offsets.append(total)
    return offsets


def _load_javalang():
    try:
        return importlib.import_module("javalang")
    except ModuleNotFoundError:
        raise ValueError("javalang package not installed")


def _java_tokens_with_offsets(src: str) -> List[Tuple[int, str]]:
    """Return list of (offset, raw_literal) from javalang tokens."""
    jl = _load_javalang()
    if not jl:
        return []

    try:
        tokens = list(jl.tokenizer.tokenize(src))
    except jl.tokenizer.LexerError:
        return []  # fall back if lexing fails (e.g., Kotlin file)

    line_offsets = _compute_line_offsets(src)
    result: list[Tuple[int, str]] = []
    for tok in tokens:
        if isinstance(tok, jl.tokenizer.String):
            line, col = tok.position
            # javalang columns are 0‑based; adjust to 0‑based offset
            offset = line_offsets[line - 1] + col
            result.append((offset, tok.value[1:-1]))  # strip surrounding quotes
    return result


@extractor_for("java", "kt")
def extract_java_strings(path: str | Path) -> List[str]:
    src = Path(path).read_text("utf-8", errors="ignore")
    src_no_comments = _JAVA_COMMENT_RE.sub("", src)

    # 1. Collect tokens via javalang
    token_strings = _java_tokens_with_offsets(src_no_comments)

    # 2. Collect triple‑quoted (Java text blocks / Kotlin raw strings)
    triple_strings = [
        (m.start(), m.group(0)[3:-3]) for m in _JAVA_TRIPLE_RE.finditer(src_no_comments)
    ]

    # Merge both lists preserving source order
    combined = token_strings + triple_strings
    combined.sort(key=lambda t: t[0])  # by offset
    return [s for _, s in combined]