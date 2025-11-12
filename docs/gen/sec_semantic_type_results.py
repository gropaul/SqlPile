from src.config import get_con


def semantic_type_example():
    con = get_con(read_only=True)

    MAX_CHARS = 200
    MAX_EXAMPLES_PER_TYPE = 5
    MAX_VALUES_PER_EXAMPLE = 3

    # First, get all combinations with their examples and percentages
    df = con.sql(f"""
            WITH ranked_columns AS (
                SELECT
                    semantic_type_llm,
                    semantic_type_llm_subtype,
                    tables.table_name as table_name,
                    columns.column_name as column_name,
                    columns.id as column_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY semantic_type_llm, semantic_type_llm_subtype
                        ORDER BY columns.id
                    ) as rn
                FROM columns
                JOIN tables ON tables.id = columns.table_id
                JOIN table_values_count as tvc ON tvc.table_id = tables.id
                WHERE tvc.count > 4000
                ORDER BY hash(column_id)
            ),
            examples AS (
                SELECT
                    rc.semantic_type_llm,
                    rc.semantic_type_llm_subtype,
                    rc.table_name,
                    rc.column_name,
                    LIST_DISTINCT(LIST(('"' || cv.value || '"') ORDER BY cv.column_id))[:{MAX_VALUES_PER_EXAMPLE}] as sample_values
                FROM ranked_columns rc
                JOIN column_values cv ON rc.column_id = cv.column_id
                WHERE rc.rn <= {MAX_EXAMPLES_PER_TYPE}
                GROUP BY rc.semantic_type_llm, rc.semantic_type_llm_subtype, rc.table_name, rc.column_name, rc.rn
            ),
            totals AS (
                SELECT
                    COUNT(*) as total_columns
                FROM columns
                WHERE semantic_type_llm IS NOT NULL
            ),
            type_counts AS (
                SELECT
                    semantic_type_llm,
                    COUNT(*) as type_count
                FROM columns
                WHERE semantic_type_llm IS NOT NULL
                GROUP BY semantic_type_llm
            ),
            subtype_counts AS (
                SELECT
                    semantic_type_llm,
                    semantic_type_llm_subtype,
                    COUNT(*) as subtype_count
                FROM columns
                WHERE semantic_type_llm IS NOT NULL
                    AND semantic_type_llm_subtype IS NOT NULL
                GROUP BY semantic_type_llm, semantic_type_llm_subtype
            )
            SELECT
                e.semantic_type_llm,
                e.semantic_type_llm_subtype,
                ROUND(100.0 * tc.type_count / t.total_columns, 2) as type_percentage,
                ROUND(100.0 * sc.subtype_count / tc.type_count, 2) as subtype_percentage,
                LIST(
                    e.table_name || '.' || e.column_name || ': ' ||
                    array_to_string(e.sample_values, ', ')
                ) as examples
            FROM examples e
            CROSS JOIN totals t
            JOIN type_counts tc ON e.semantic_type_llm = tc.semantic_type_llm
            JOIN subtype_counts sc ON e.semantic_type_llm = sc.semantic_type_llm
                AND e.semantic_type_llm_subtype = sc.semantic_type_llm_subtype
            WHERE
                e.semantic_type_llm_subtype != 'Other'
                AND e.semantic_type_llm != 'Test'
                AND e.semantic_type_llm IS NOT NULL
                AND e.semantic_type_llm_subtype IS NOT NULL
            GROUP BY e.semantic_type_llm, e.semantic_type_llm_subtype, tc.type_count, sc.subtype_count, t.total_columns
            ORDER BY e.semantic_type_llm, e.semantic_type_llm_subtype
        """).df()

    # Create a markdown table
    markdown_lines = []
    markdown_lines.append("| Semantic Type | Subtype | Examples |")
    markdown_lines.append("|---------------|---------|----------|")
    for _, row in df.iterrows():
        semantic_type = row['semantic_type_llm']
        subtype = row['semantic_type_llm_subtype']
        type_pct = row['type_percentage']
        subtype_pct = row['subtype_percentage']
        examples_small = [ex[:MAX_CHARS] + ("..." if len(ex) > MAX_CHARS else "") for ex in row['examples']]
        examples = "<br>".join(examples_small)
        markdown_lines.append(f"| {semantic_type} ({type_pct}%) | {subtype} ({subtype_pct}%) | {examples} |")
    markdown_content = "\n".join(markdown_lines)
    with open("semantic_type_example.md", "w") as f:
        f.write(markdown_content)




if __name__ == "__main__":
    semantic_type_example()