import matplotlib.pyplot as plt
from typing import Dict, Tuple

from src.sql_analysis.expression_analysis.model import ExpressionAggregateDict


def plot_expression_tree_matplotlib(
    root: ExpressionAggregateDict,
    figsize: Tuple[int, int] = (12, 8),
) -> None:
    fig, ax = plt.subplots(figsize=figsize)
    ax.axis("off")

    # --- layout computation ---

    def subtree_width(node: ExpressionAggregateDict) -> int:
        children = node.get("children", [])
        if not children:
            return 1
        return sum(subtree_width(c) for c in children)

    def assign_positions(
        node: ExpressionAggregateDict,
        x: float,
        y: float,
        width: float,
        positions: Dict[int, Tuple[float, float]],
        level_gap: float = 1.5,
    ):
        positions[id(node)] = (x, y)

        children = node.get("children", [])
        if not children:
            return

        total = sum(subtree_width(c) for c in children)
        cur_x = x - width / 2

        for child in children:
            w = subtree_width(child) / total * width
            cx = cur_x + w / 2
            cy = y - level_gap
            assign_positions(child, cx, cy, w, positions)
            cur_x += w

    positions: Dict[int, Tuple[float, float]] = {}
    total_width = subtree_width(root)
    assign_positions(root, 0.0, 0.0, total_width, positions)

    # --- drawing ---

    def draw_node(node: ExpressionAggregateDict):
        x, y = positions[id(node)]

        label = (
            f"{node['expression_class']}\n"
            f"{node['expression_type']}\n"
            f"count={node['count']}"
        )

        ax.text(
            x,
            y,
            label,
            ha="center",
            va="center",
            bbox=dict(boxstyle="round,pad=0.3"),
        )

        for child in node.get("children", []):
            cx, cy = positions[id(child)]
            ax.plot([x, cx], [y - 0.15, cy + 0.15])
            draw_node(child)

    draw_node(root)

    ax.relim()
    ax.autoscale_view()
    plt.show()
