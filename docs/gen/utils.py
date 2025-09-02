import re


def get_figure(path: str, caption: str = "", description: str = "", label: str = "") -> str:
    """
    Generate a LaTeX figure environment including an image, optional caption, and description.

    Parameters
    ----------
    path : str
        The full path (with extension) to the figure file.
    caption : str, optional
        The caption text to display under the figure.
    description : str, optional
        The description text for accessibility (screen readers).
    label: str, optional


    Returns
    -------
    str
        A LaTeX string representing the figure environment.
    """

    if not description and label:
        description = label

    template = f"""\\begin{{figure}}
  \\centering
  \\includegraphics[width=\\linewidth]{{{path}}}
  {"\\caption{" + caption + "}" if caption else ""}
  {"\\Description{" + description + "}" if description else ""}
    {"\\label{" + label + "}" if label else ""}
\\end{{figure}}"""
    return template


def escape_latex_string(latex_str: str) -> str:
    # % -> \%, { and } -> {{ and }}, \ to \\

    latex_str.replace('%', r'\%')
    latex_str = latex_str.replace('{', r'{{').replace('}', r'}}')
    latex_str = latex_str.replace('\\', r'\\')
    return latex_str


def format_latex_string(latex_str: str) -> str:
    """
    Format LaTeX string:
      - Replace inline code `...` with \texttt{...}
      - Escape %
      - Convert one-level Markdown lists into LaTeX itemize/enumerate
    """
    # Inline code
    formatted = re.sub(r'`([^`]+)`', r'\\texttt{\1}', latex_str)

    # Escape %
    formatted = formatted.replace('%', r'\%')

    lines = formatted.splitlines()
    out_lines = []
    in_list = False
    list_type = None

    unordered_pat = re.compile(r'^\s*[-*+]\s+(.*)$')
    ordered_pat   = re.compile(r'^\s*\d+[.)]\s+(.*)$')

    for line in lines:
        if unordered_pat.match(line):
            content = unordered_pat.match(line).group(1)
            if not in_list:
                out_lines.append(r"\begin{itemize}")
                in_list = True
                list_type = "itemize"
            out_lines.append(r"\item " + content)
        elif ordered_pat.match(line):
            content = ordered_pat.match(line).group(0).split(maxsplit=1)[1]
            if not in_list:
                out_lines.append(r"\begin{enumerate}")
                in_list = True
                list_type = "enumerate"
            out_lines.append(r"\item " + content)
        else:
            if in_list:
                out_lines.append(rf"\end{{{list_type}}}")
                in_list = False
                list_type = None
            out_lines.append(line)

    if in_list:
        out_lines.append(rf"\end{{{list_type}}}")

    return "\n".join(out_lines)



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