
import json
from typing import Optional, List, Dict, Callable

from src.config import get_con
from typing import TypedDict, List

from src.sql_analysis.expression_analysis.model import ExpressionDict, ExpressionAggregateDict, NextOption
from src.sql_analysis.expression_analysis.viz import plot_expression_tree_matplotlib


def shorten_ret(type_str: str, exp: str, ret: str) -> str:
    if 'INT' in ret:
        return 'INT'
    if ret.startswith('VARCHAR'):
        return 'TEXT'
    if ret.startswith('CHAR'):
        return 'CHAR'
    if ret.startswith('FLOAT'):
        return 'FLOAT'
    if ret.startswith('DOUBLE'):
        return 'DOUBLE'
    if ret.startswith('BOOLEAN'):
        return 'BOOLEAN'
    if ret.startswith('DECIMAL'):
        return 'DECIMAL'
    return ret


def simplify_exp(exp: str) -> str:
    # if there is a (, remove everything after it
    # remove all "
    exp = exp.replace('"', '')
    if '(' in exp:
        exp = exp.split('(')[0]

    if 'count_star' == exp:
        return 'count(*)'
    return exp

def short_type(type_str: str, exp: str, ret: str) -> str:
    ret_simple = shorten_ret(type_str, exp, ret)
    exp_simple = simplify_exp(exp)
    if type_str == 'BOUND_REF':
        return f'col_{{{ret_simple}}}'
    if type_str == 'CONSTANT':
        return f'const_{{{ret_simple}}}'
    if type_str == 'BOUND_FUNCTION':
        return exp_simple
    if type_str == 'EQUAL':
        return '='
    if type_str == 'NOTEQUAL':
        return '!='
    if type_str == 'NOT_DISTINCT_FROM':
        return 'is'
    if type_str == 'DISTINCT_FROM':
        return 'is not'
    if type_str == 'GREATERTHAN':
        return '>'
    if type_str == 'LESSTHAN':
        return '<'
    if type_str == 'GREATERTHANOREQUALTO':
        return '>='
    if type_str == 'LESSTHANOREQUALTO':
        return '<='
    if type_str == 'BOUND_AGGREGATE':
        return exp_simple
    return type_str


def simplify_expression(exp: ExpressionDict) -> str:
    n_children = len(exp.get('children', []))
    type_simpled = short_type(exp['expression_type'], exp['expression'], exp['return_type'])
    if n_children == 0:
        return type_simpled

    if n_children == 1:
        child = simplify_expression(exp['children'][0])
        return f"{type_simpled}({child})"

    else :
        # return exp + exp e.g.
        childs = [simplify_expression(child) for child in exp['children']]

        if len(type_simpled) <= 2:
            return f' {type_simpled} '.join(childs)
        else:
            return f"{type_simpled}(" + ', '.join(childs) + ")"



def join_condition_to_expression(condition: Dict) -> ExpressionDict:
    comparison = condition['comparison']
    left: ExpressionDict = condition['left']
    right: ExpressionDict = condition['right']

    return {
        'expression': f"{left} = {right}",
        'expression_type': comparison,
        'expression_class': 'BOUND_COMPARISON',
        'return_type': 'BOOLEAN',
        'children': [left, right],
    }


def process_node(op: Dict):
    if op['operator_type'] == 'COMPARISON_JOIN':
        process_join(op)
    if op['operator_type'] == 'AGGREGATE':
        process_agg(op)


JOIN_EXPRESSIONS = []
AGG_EXPRESSIONS = []

def process_join(op: Dict):
    join_conditions = op.get('join_conditions', [])
    for condition in join_conditions:
        expression = join_condition_to_expression(condition)
        JOIN_EXPRESSIONS.append(expression)


def process_agg(op: Dict):
    agg_expressions = op.get('expressions', [])
    for expression in agg_expressions:
        AGG_EXPRESSIONS.append(expression)



def process_expression(expression: str) -> str:
    pass

def iterate(node: dict, func: Callable[[dict], None]):
    func(node)
    if 'children' in node:
        for child in node['children']:
            iterate(child, func)


from collections import defaultdict
from typing import List, Tuple


from collections import defaultdict
from typing import List, Tuple


def aggregate_expressions(
    expressions: List[ExpressionDict],
) ->  List[NextOption]:

    next_options: List[NextOption] = []

    for expr in expressions:
        matched_option = None
        for option in next_options:
            if (option['expression_type'] == expr['expression_type'] and
                option['expression_class'] == expr['expression_class']):
                matched_option = option
                break

        if matched_option is None:
            matched_option = {
                'expression_type': expr['expression_type'],
                'expression_class': expr['expression_class'],
                'count': 0,
                'children': []
            }
            next_options.append(matched_option)

        matched_option['count'] += 1

        if expr['children']:
            child_aggregates = aggregate_expressions(expr['children'])
            for child_agg in child_aggregates:
                matched_option['children'].append(child_agg)

def main():
    get_data_sql = """
        SELECT executable_sql, ANY_VALUE(logical_plan_optimized_detailed) AS logical_plan_optimized_detailed
        FROM queries_executable 
        JOIN queries ON queries_executable.query_id = queries.id
        JOIN repos ON queries.repo_id = repos.id
        WHERE '3rd-party' NOT IN repo_url
        -- WHERE 'join' IN original_sql
        GROUP BY executable_sql
    """

    con = get_con(read_only=True)
    df = con.execute(get_data_sql).fetchdf()

    for _, row in df.iterrows():
        logical_plan = row['logical_plan_optimized_detailed']
        logical_plan = json.loads(logical_plan)
        iterate(logical_plan, process_node)


    expression_cnt = {}
    for expression in JOIN_EXPRESSIONS:
        simplified = simplify_expression(expression)
        if 'col_{TEXT}' in simplified:
            expression_cnt[simplified] = expression_cnt.get(simplified, 0) + 1

    # print top 10 expressions
    for expr, cnt in sorted(expression_cnt.items(), key=lambda x: x[1], reverse=True)[:100]:
        print(f"{expr}: {cnt}")



    exit()


    aggregated = aggregate_expressions(JOIN_EXPRESSIONS)

    print(f"Aggregated Join Expressions: {aggregated}")


if __name__ == '__main__':
    main()
