import re

def tidy_up_string(s: str) -> str:
    """
    Cleans up a string by:
      • Collapsing all runs of whitespace (spaces, tabs, newlines) to a single space
      • Stripping leading and trailing whitespace

    Example
    -------
    'Hello, World!'
    """
    return re.sub(r'\s+', ' ', s).strip()

