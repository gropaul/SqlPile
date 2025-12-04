from typing import List
from pathlib import Path
import re


def sanitize_label(text: str) -> str:
    """
    Make a LaTeX-safe label fragment:
    - lowercase
    - spaces to hyphens
    - remove characters not [a-z0-9:_-]
    - collapse duplicate hyphens/underscores
    """
    t = text.lower()
    t = t.replace(" ", "-")
    t = re.sub(r"[^a-z0-9:_\-]+", "", t)
    t = re.sub(r"[-_]{2,}", "-", t)
    return t.strip("-_:")


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

    cap_str = f"\\caption{{{caption}}}" if caption else ""
    desc_str = f"\\Description{{{description}}}" if description else ""
    label_str = f"\\label{{{label}}}" if label else ""

    template = (
        "\\begin{figure}\n"
        "  \\centering\n"
        f"  \\includegraphics[width=\\linewidth]{{{path}}}\n"
        f"  {cap_str}\n"
        f"  {desc_str}\n"
        f"  {label_str}\n"
        "\\end{figure}"
    )
    return template


def get_multi_figure(paths: List[str], caption: str, captions: List[str], label: str = "", two_column: bool = True, main_percentage: float = 0.95) -> str:
    """
    Generate a LaTeX figure with multiple subfigures.
    Each subfigure gets a label derived from its file name.

    Parameters
    ----------
    paths : List[str]
        Full paths (with extension) to the figure files.
    captions : List[str]
        Captions per subfigure (same length as paths; use "" for none).
    label : str, optional
        Overall figure label prefix. Subfigure labels will be:
        '<label>:<filename-stem>' if provided, otherwise 'fig:<filename-stem>'.

    Returns
    -------
    str
        LaTeX code for the multi-subfigure environment.
    """

    if len(paths) != len(captions):
        raise ValueError("Number of paths and captions must match.")

    overall_label_prefix = sanitize_label(label) if label else "fig"

    percentage_per_figure = round(main_percentage / len(paths), 4)

    subfigures = []
    for path, cap in zip(paths, captions):
        stem = Path(path).stem
        stem_safe = sanitize_label(stem)
        sub_label = f"{overall_label_prefix}:{stem_safe}"
        cap_str = f"\\caption{{{cap}}}" if cap else ""

        subfig = (
            f"\\begin{{subfigure}}{{{percentage_per_figure}\\linewidth}}\n"
            "   \\centering\n"
            f"  \\includegraphics[width=\\linewidth]{{{path}}}\n"
            f"  {cap_str}\n"
            f"  \\label{{{sub_label}}}\n"
            "\\end{subfigure}"
        )
        subfigures.append(subfig)

    overall_label_str = f"\\label{{{overall_label_prefix}}}" if label else ""
    cap_str = f"\\caption{{{caption}}}" if caption else ""

    figure_text = "figure" if not two_column else "figure*"
    backslash_n = "\n"
    position_identifier = "[h!]"
    template = (
        f"\\begin{{{figure_text}}}{position_identifier}\n"
        "  \\centering\n"
        f"{backslash_n.join(subfigures)}\n"
        f"  {overall_label_str}\n"
        f"  {cap_str}\n"
        f"\\end{{{figure_text}}}"
    )
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

    # Escape #
    formatted = formatted.replace('#', r'\#')

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


def join_list_v2(values: List[str]) -> str:
    """
    Join a list of strings with commas, and 'and' before the last item.
    """
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    return ", ".join(values[:-1]) + " and " + values[-1]

def join_list(lst: list, final_word: str = '') -> str:
    """
    Join a list of strings with commas, and 'and' before the last item.
    """
    if not lst:
        return ""
    if len(lst) == 1:
        return lst[0]
    return ", ".join(lst[:-1]) + " and " + lst[-1] + f" {final_word.strip()}" if final_word else ""