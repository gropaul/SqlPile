import re

def format_latex_string(latex_str: str) -> str:
    """
    Format a LaTeX string by replacing all occurrences of `VARIABLE` with \texttt{VARIABLE}.
    """
    formatted_string = re.sub(r'`([^`]+)`', r'\\texttt{\1}', latex_str)

    # escape % as they are used for comments in LaTeX
    formatted_string = formatted_string.replace('%', r'\%')
    return formatted_string



def format_number(number: int) -> str:
    """
    Format a number with commas as thousands separators.
    """
    # if number does not have any digits after comma, return it as an integer
    if number == int(number):
        return f"{int(number):,}"
    return f"{number:,}"


def join_list_with_and(lst: list, final_word: str = '') -> str:
    """
    Join a list of strings with commas, and 'and' before the last item.
    """
    if not lst:
        return ""
    if len(lst) == 1:
        return lst[0]
    return ", ".join(lst[:-1]) + " and " + lst[-1] + f" {final_word.strip()}" if final_word else ""