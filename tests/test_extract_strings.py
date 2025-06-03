"""test_string_extractors.py

Pytest suite covering the multi‑language extractors implemented in
*string_extractors.py* (v1.2).

Each parametrised test case writes a small source snippet to a temporary file,
invokes `extract_strings`, and checks that the expected list of string literals
is returned—multi‑line content, escape handling, concatenation logic, and the
optional `dedupe` behaviour included.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from extract_strings import extract_strings

# ---------------------------------------------------------------------------
# Parametrised snippets
# ---------------------------------------------------------------------------

CASES = [
    # ---------------------------------------------------------------------
    # Python – triple‑quoted multiline plus single quotes
    # ---------------------------------------------------------------------
    (
        '''x = """multi
line"""
y = 'single'
''',
        "py",
        ["multi\nline", "single"],
    ),
    # ---------------------------------------------------------------------
    # JavaScript – template literal and double quotes
    # ---------------------------------------------------------------------
    (
        """const tmpl = `hello\nworld`\nconst s = \"foo\";\n""",
        "js",
        ["hello\nworld", "foo"],
    ),
    # ---------------------------------------------------------------------
    # Java – text block and ordinary string
    # ---------------------------------------------------------------------
    (
        """class T {\n    String block = \"\"\"first\nsecond\"\"\";\n    String one = \"hi\";\n}\n""",
        "java",
        ["first\nsecond", "hi"],
    ),
    # ---------------------------------------------------------------------
    # Go – back‑tick raw string and double‑quoted string
    # ---------------------------------------------------------------------
    (
        """package main\nvar sql = `SELECT *\nFROM table`\nvar s = \"bar\"\n""",
        "go",
        ["SELECT *\nFROM table", "bar"],
    ),
    # ---------------------------------------------------------------------
    # C++ – raw string literal and regular string
    # ---------------------------------------------------------------------
    (
        """const char* r = R"(hello\ncpp)";\nconst char* s = \"baz\";\n""",
        "cpp",
        ["hello\ncpp", "baz"],
    ),
    # ---------------------------------------------------------------------
    # Java: Multiline string joined by +
    # ---------------------------------------------------------------------
    (
        """String s = \"\"\"This is a\nmultiline string \"\"\" +\n\"and this is another part\";\n""",
        "java",
        ["This is a\nmultiline string and this is another part"],
    ),
    # ---------------------------------------------------------------------
    # NEW 1: Python – raw strings with backslashes (no escape processing)
    # ---------------------------------------------------------------------
    (
        r'''path = r"C:\\new\\test.txt"\ntext = r'raw\\nlines'\n''',
        "py",
        ["C:\\new\\test.txt", "raw\\nlines"],
    ),
    # ---------------------------------------------------------------------
    # NEW 2: JavaScript – template literal with interpolation placeholder
    # ---------------------------------------------------------------------
    (
        "const name = 'Bob';\nconst greet = `Hello, ${name}!`;\n",
        "js",
        ["Hello, ${name}!"] ,
    ),
    # ---------------------------------------------------------------------
    # NEW 3: Java – chain of three concatenated literals
    # ---------------------------------------------------------------------
    (
        """class T { String msg = \"\"\"Line1\nLine2\"\"\" + \" \" + \"Line3\"; }""",
        "java",
        ["Line1\nLine2 Line3"],
    ),
    # ---------------------------------------------------------------------
    # NEW 4: Go – concatenation across raw and interpreted strings
    # ---------------------------------------------------------------------
    (
        "package main\nvar q = `SELECT ` + \"id, name\" + ` FROM users`;\n",
        "go",
        ["SELECT id, name FROM users"],
    ),
    # ---------------------------------------------------------------------
    # NEW 5: C++ – adjacent string literals concatenated implicitly
    # ---------------------------------------------------------------------
    (
        "const char* w = \"wide\" \" string\";\n",
        "cpp",
        ["wide string"],
    ),
]

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("snippet, ext, expected", CASES)
def test_extract_strings(tmp_path: Path, snippet: str, ext: str, expected: list[str]):
    """Generic test: write *snippet* to temp file and assert extraction matches."""

    fname = tmp_path / f"sample.{ext}"
    fname.write_text(snippet, encoding="utf-8")

    # Primary extraction
    got = extract_strings(fname)
    assert got == expected

    # Check dedupe flag does not alter unique list
    got_dedupe = extract_strings(fname, dedupe=True)
    assert got_dedupe == expected

    # Inject a duplicate and ensure dedupe trims it
    snippet2 = snippet + "\n" + snippet  # duplicate literals appear twice
    fname2 = tmp_path / f"sample2.{ext}"
    fname2.write_text(snippet2, encoding="utf-8")
    got2 = extract_strings(fname2, dedupe=False)
    got2_dedupe = extract_strings(fname2, dedupe=True)
    assert len(got2) == 2 * len(expected)
    assert got2_dedupe == expected

# ---------------------------------------------------------------------------
# Additional test: ensure dedupe preserves order
# ---------------------------------------------------------------------------


def test_dedupe_preserves_order(tmp_path: Path):
    """Duplicate strings should be removed *and* original order preserved."""

    snippet = "x = \"one\"\ny = \"two\"\nprint(\"one\")\n"  # 'one' repeats later
    src = tmp_path / "dup.py"
    src.write_text(snippet, encoding="utf-8")

    assert extract_strings(src, dedupe=False) == ["one", "two", "one"]
    assert extract_strings(src, dedupe=True) == ["one", "two"]
