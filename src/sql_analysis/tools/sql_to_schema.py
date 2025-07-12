from dataclasses import dataclass
from typing import List

from sqloxide import parse_sql
from tree_sitter_language_pack import get_parser

from src.sql_analysis.tools.parse_sql import print_recursive
from src.sql_analysis.tools.sql_types import unify_type


@dataclass
class ColumnInfo:
    """Represents column metadata."""
    name: str
    type: str
    table_index: int
    is_primary_key: bool = False


@dataclass
class TableSchema:
    """Represents the schema of a table."""
    table_name: str
    columns: List[ColumnInfo]


categories_table = """
                   create table `categories`
                   (
                       `id`                bigint(20) unsigned    not null,
                       `name`              varchar(256)           not null default 'unknown',
                       `disabled`          tinyint(1) unsigned    not null default 0,
                       `weight`            decimal(3, 2) unsigned not null default 1.00,
                       `selection_count`   bigint(20) unsigned             default 0 comment 'number of times this category has been chosen to be played on its own ( regardless of if it was allowed ) ',
                       `questions_asked`   bigint(20) unsigned             default 0,
                       `translation_dirty` tinyint(1) unsigned             default 0,
                       `trans_tr`          text                            default null,
                       `trans_pt`          text                            default null,
                       `trans_fr`          text                            default null,
                       `trans_es`          text                            default null,
                       `trans_de`          text                            default null,
                       `trans_hi`          text                            default null,
                       `trans_sv`          text                            default null,
                       `trans_ru`          text                            default null,
                       `trans_pl`          text                            default null,
                       `trans_it`          text                            default null,
                       `trans_ja`          text                            default null,
                       `trans_ko`          text                            default null,
                       `trans_bg`          text                            default null,
                       `trans_zh`          text                            default null,
                       `trans_ar`          text                            default null,
                       `trans_nl`          text                            default null
                   ) engine = innodb
                     default charset = utf8mb4 \
                   """

premiumn_plans = """create table `premium_plans`
                    (
                        `id`          varchar(250) character set ascii not null comment 'from chargebee',
                        `lifetime`    tinyint(1) unsigned              not null default 0 comment 'true if the plan is charged only once',
                        `name`        varchar(250)                     not null comment 'plan name',
                        `price`       decimal(5, 2) unsigned           not null comment 'price',
                        `period`      int(10) unsigned                 not null,
                        `period_unit` enum ( 'year' , 'month' )        not null default 'month',
                        `currency`    enum ( 'gbp' , 'usd' )           not null default 'gbp',
                        `cache_date`  datetime                         not null
                    ) engine = innodb
                      default charset = utf8mb4 comment = 'plans cached from chargebee api' \
                 """

scheduled_games_table = """
                        create table schema.scheduled_games
                        (
                            id               bigint(20) unsigned not null primary key,
                            guild_id         bigint(20) unsigned not null,
                            channel_id       bigint(20) unsigned not null,
                            user_id          bigint(20) unsigned not null,
                            quickfire        tinyint(1) unsigned not null default 0,
                            questions        int(11)             not null,
                            start_time       time                not null,
                            queuetime        datetime            not null default current_timestamp(),
                            hintless         tinyint(1) unsigned          default 0,
                            category         varchar(250)                 default null,
                            announce_mins    int(10) unsigned             default null comment 'minutes before game to announce it',
                            announce_message text                         default null,
                            announce_ping    bigint(20) unsigned          default null,
                            announce_time    time generated always as ( cast(from_unixtime(time_to_sec(start_time) + 82800 - 60 * announce_mins) as time) ) virtual
                        ) engine = innodb
                          default charset = utf8mb4"""

trivia_user_cache = """create table `trivia_user_cache`
                       (
                           `snowflake_id`   bigint(20) unsigned      not null comment 'snowflake id pk',
                           `username`       varchar(700)             not null,
                           `discriminator`  int(4) unsigned zerofill not null,
                           `icon`           varchar(256)             not null,
                           `rankcard_theme` bigint(20) unsigned default null comment 'rankcard theme , a shop item id or null for default'
                       )
                    """


def test_parse_create_categories_table():
    schema = parse_create_table(categories_table)
    expected_column_names = [
        "id", "name", "disabled", "weight", "selection_count", "questions_asked", "translation_dirty", "trans_tr",
        "trans_pt", "trans_fr", "trans_es", "trans_de", "trans_hi", "trans_sv", "trans_ru", "trans_pl", "trans_it",
        "trans_ja", "trans_ko", "trans_bg", "trans_zh", "trans_ar", "trans_nl"
    ]

    print(schema)
    # print the missing columns
    missing_columns = [col for col in expected_column_names if col not in [c.name for c in schema.columns]]
    if missing_columns:
        print(f"Missing columns: {missing_columns}")
    assert schema.table_name == 'categories'
    assert len(schema.columns) == len(expected_column_names)
    for column in schema.columns:
        assert column.name in expected_column_names
        assert isinstance(column.type, str), f"Column {column.name} has type {column.type} which is not a string"


from simple_ddl_parser import DDLParser


def parse_with_simple_ddl_parser(sql: str) -> TableSchema:

    parsed = DDLParser(sql).run()[0]

    table_name = parsed['table_name']

    columns: List[ColumnInfo] = []
    for (index, col) in enumerate(parsed['columns']):
        name = col['name']
        data_type = col['type']
        is_primary_key = col.get('is_primary_key', False)
        columns.append(
            ColumnInfo(
                name=name,
                type=data_type,
                table_index=index,
                is_primary_key=is_primary_key
            )
        )

    return TableSchema(table_name=table_name, columns=columns)


def test_cbo_t3():
    query = """
            create table cbo_t3
            (
                key string not null primary key,
                value string,
                c_int     int,
                c_float   float,
                c_boolean boolean
            ) row format delimited fields terminated by ' , ' stored as textfile
            """

    schema = parse_create_table(query)

    expected_column_names = ['key', 'value', 'c_int', 'c_float', 'c_boolean']
    print(schema)
    # print the missing columns
    missing_columns = [col for col in expected_column_names if col not in [c.name for c in schema.columns]]
    if missing_columns:
        print(f"Missing columns: {missing_columns}")
    assert schema.table_name == 'cbo_t3'
    assert len(schema.columns) == len(expected_column_names)

def test_parse_create_scheduled_games_table():
    schema = parse_create_table(scheduled_games_table)
    expected_column_names = [
        'id', 'guild_id', 'channel_id', 'user_id', 'quickfire',
        'questions', 'start_time', 'queuetime', 'hintless',
        'category', 'announce_mins', 'announce_message',
        'announce_ping', 'announce_time'
    ]

    print(schema)

    # print the missing columns
    missing_columns = [col for col in expected_column_names if col not in [c.name for c in schema.columns]]
    if missing_columns:
        print(f"Missing columns: {missing_columns}")

    assert schema.table_name == 'schema.scheduled_games'
    assert len(schema.columns) == len(expected_column_names)

    for column in schema.columns:
        assert column.name in expected_column_names
        assert isinstance(column.type, str)


def test_repo_16784():
    query = """
            create table browsertracker
            (
                browsertrackerid bigint not null primary key,
                userid           bigint,
                browserkey       bigint
            ) engine innodb
            """
    schema = parse_create_table(query)
    expected_column_names = ['browsertrackerid', 'userid', 'browserkey']
    print(schema)


def test_repo_6406():
    query = """
            create table `sys_dashboard`
            (
                `id`      int(11) not null auto_increment,
                `id_form` int(11) null default null,
                `id_user` int(11) null default null,
                primary key (`id`) using btree,
                index `i_user` (`id_user`) using btree,
                index `i_id` (`id`) using btree
            ) engine = innodb
              auto_increment = 4
              character set = latin1
              collate = latin1_swedish_ci
              row_format = compact \
            """

    schema = parse_create_table(query)
    expected_column_names = ['id', 'id_form', 'id_user']
    print(schema)


def test_repo_21581():
    query = """
            create table public.comments
            (
                id             bigint not null            default nextval('comments_id_seq'::regclass),
                content        text collate pg_catalog."" default "" not null,
                publication_id bigint not null,
                user_id        bigint not null,
                created_at     timestamp(0) without time zone,
                updated_at     timestamp(0) without time zone,
                constraint comments_pkey primary key (id),
                constraint comments_publication_id_foreign foreign key (publication_id) references public.publications (id) match simple on update no action on delete no action,
                constraint comments_user_id_foreign foreign key (user_id) references public.users (id) match simple on update no action on delete no action
            )
            with ( oids = false) tablespace pg_default"
            """

    schema = parse_create_table(query)

    expected_column_names = [
        'id', 'content', 'publication_id', 'user_id', 'created_at', 'updated_at'
    ]
    print(schema)

    # print the missing columns
    assert schema.table_name == 'public.comments'
    assert len(schema.columns) == len(expected_column_names)

    missing_columns = [col for col in expected_column_names if col not in [c.name for c in schema.columns]]
    assert len(missing_columns) == 0, f"Missing columns: {missing_columns}"


def test_repo_21641():
    query = """
            create table movies
            (
                id            int           not null primary key,
                title         nvarchar(255) not null,
                directorid    int foreign key references directors ( id ),
                genreid       int foreign key references genres ( id ),
                categoryid    int foreign key references categories ( id ),
                copyrightyear int,
                length        varchar(50),
                rating        int,
                notes         varchar(50)
            ) engine = innodb
            """
    schema = parse_create_table(query)
    expected_column_names = [
        'id', 'title', 'directorid', 'copyrightyear', 'length',
        'genreid', 'categoryid', 'rating', 'notes'
    ]
    print(schema)


def test_repo_6():
    query = "create table .ptned ( a string ) partitioned by ( b int ) stored as textfile"
    schema = parse_create_table(query)
    expected_column_names = ['a', 'b']
    print(schema)


def test_repo_6_2():
    query = "create table dest1 ( key string comment 'default' , value string comment 'default' )"
    schema = parse_create_table(query)
    expected_column_names = ['key', 'value']
    print(schema)

    missing_columns = [col for col in expected_column_names if col not in [c.name for c in schema.columns]]
    assert len(missing_columns) == 0, f"Missingcolumns: {missing_columns}"
    assert schema.table_name == 'dest1'
    assert len(schema.columns) == len(expected_column_names)


def test_repo_6_3():
    query = "create table dest1 ( a2 string, a1 string )"
    schema = parse_create_table(query)
    expected_column_names = ['key', 'value']
    print(schema)


def test_premiumn_plans():
    schema = parse_create_table(premiumn_plans)
    expected_column_names = [
        'id', 'lifetime', 'name', 'price', 'period',
        'period_unit', 'currency', 'cache_date'
    ]

    print(schema)

    # print the missing columns
    missing_columns = [col for col in expected_column_names if col not in [c.name for c in schema.columns]]
    if missing_columns:
        print(f"Missing columns: {missing_columns}")

    assert schema.table_name == 'premium_plans'
    assert len(schema.columns) == len(expected_column_names)

    for column in schema.columns:
        assert column.name in expected_column_names
        assert isinstance(column.type, str)


def test_trivia_user_cache():
    schema = parse_create_table(trivia_user_cache)
    expected_column_names = [
        'snowflake_id', 'username', 'discriminator',
        'icon', 'rankcard_theme'
    ]

    print(schema)

    # print the missing columns
    missing_columns = [col for col in expected_column_names if col not in [c.name for c in schema.columns]]
    if missing_columns:
        print(f"Missing columns: {missing_columns}")

    assert schema.table_name == 'trivia_user_cache'
    assert len(schema.columns) == len(expected_column_names)

    for column in schema.columns:
        assert column.name in expected_column_names
        assert isinstance(column.type, str)


def get_data_type(statement_col: dict) -> str:
    data_type_statement = statement_col.get('data_type')
    # if it is a string, return it directly
    if isinstance(data_type_statement, str):
        return data_type_statement

    # if it is a dict, return the first key
    return list(statement_col['data_type'].keys())[0]


import re


def remove_table_options(sql: str) -> str:
    """
    Removes the table options from a CREATE TABLE SQL statement.
    """
    start = sql.find('(')
    if start == -1:
        return sql  # no opening parenthesis, return as-is

    depth = 0
    for i in range(start, len(sql)):
        if sql[i] == '(':
            depth += 1
        elif sql[i] == ')':
            depth -= 1
            if depth == 0:
                # Found the closing parenthesis of the column definitions
                return sql[:i + 1]  # Keep everything up to and including this ')'

    return sql  # fallback if unmatched


import re

def drop_constraints(sql: str) -> str:
    """
    Strip all explicit `CONSTRAINT …` clauses from a CREATE TABLE statement.

    Parameters
    ----------
    sql : str
        Original CREATE TABLE statement (any formatting).

    Returns
    -------
    str
        Same statement with every constraint definition removed, plus
        cosmetic cleanup so commas and parentheses stay valid.
    """
    # 1. Remove each `constraint …` segment (greedy to the next comma or `)`)
    without_constraints = re.sub(
        r"""
        ,?                         # optional leading comma
        \s*constraint\s+           # CONSTRAINT keyword
        [^\(\),]+?                 # constraint name + optional type
        (?:\([^\)]*\))?            # column-list / check expression in (...)
        (?:\s+references[^\),]+)?  # possible REFERENCES clause
        (?:
            on\s+update[^\),]+ |   # optional ON UPDATE …
            on\s+delete[^\),]+     # optional ON DELETE …
        )*                         # … maybe repeated
        """,
        "",
        sql,
        flags=re.IGNORECASE | re.DOTALL | re.VERBOSE,
    )

    # 2. Collapse duplicate commas that step 1 can leave behind
    cleaned = re.sub(r",\s*,", ", ", without_constraints)

    # 3. Remove a trailing comma just before the closing parenthesis
    cleaned = re.sub(r",\s*\)", ")", cleaned)

    return cleaned


def rewrite_sql_for_parsing(sql: str) -> str:
    # Match variations like:
    # decimal(5,2) unsigned
    # decimal (5 , 2 ) unsigned
    # decimal( 5 ,2)   unsigned
    pattern = re.compile(r'decimal\s*\(\s*\d+\s*,\s*\d+\s*\)\s*unsigned', re.IGNORECASE)

    # if there are [ or ] in the sql, we need to replace them with "
    if '[' in sql or ']' in sql:
        sql = sql.replace('[', '"').replace(']', '"')

    # Replace with just the decimal definition, removing 'unsigned'
    def replacer(match):
        # Extract the decimal part without 'unsigned'
        decimal_part = re.search(r'decimal\s*\(\s*\d+\s*,\s*\d+\s*\)', match.group(0), re.IGNORECASE)
        return decimal_part.group(0) if decimal_part else match.group(0)

    # remove unsigned also from floats : float unsigned -> float
    sql = re.sub(r'\bfloat\s+unsigned\b', 'float', sql, flags=re.IGNORECASE)

    # remove all "signed" specifiers -> Default is signed, so we can remove it
    sql = re.sub(r'\bsigned\b', '', sql, flags=re.IGNORECASE)

    sql = pattern.sub(replacer, sql)

    # remove everything that comes after "... engine ="
    sql = sql.split(' engine =')[0]

    # the parser does not support the "zerofill" specifier, so we remove it
    sql = re.sub(r'\bzerofill\b', '', sql, flags=re.IGNORECASE)

    sql = remove_table_options(sql)  # Remove table options like ENGINE, CHARSET, etc.

    sql = drop_constraints(sql)  # Remove constraints like PRIMARY KEY, FOREIGN KEY, etc.

    # rewrite serial data types to integer, same for smallserial and bigserial
    sql = re.sub(r'\bserial\b', 'int', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bsmallserial\b', 'smallint', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bbigserial\b', 'bigint', sql, flags=re.IGNORECASE)
    return sql


def parse_create_table_with_tree_sitter(sql: str) -> TableSchema:
    parser = get_parser('sql')
    tree = parser.parse(sql.encode('utf-8'))

    # check if there was an error during parsing
    if tree.root_node is None:
        raise ValueError("Failed to parse the SQL statement with Tree-sitter. The syntax might be incorrect.")

    statement = tree.root_node.children[0]  # Assuming the first child is the CREATE TABLE statement
    create_table = statement.children[0]  # The first child should be the CreateTable node

    if create_table.type != 'create_table':
        raise ValueError("The provided SQL statement is not a valid CREATE TABLE statement.")

    table_reference = [child for child in create_table.children if child.type == 'object_reference'][0]
    column_definitions = [child for child in create_table.children if child.type == 'column_definitions'][0]
    column_definitions = [child for child in column_definitions.children if child.type == 'column_definition']
    table_name = table_reference.text.strip().lower().decode('utf-8')

    columns: List[ColumnInfo] = []

    for definition in column_definitions:
        identifier = [child for child in definition.children if child.type == 'identifier'][0]
        column_name = identifier.text.strip().decode('utf-8')

        type = definition.children[1].text.strip().decode('utf-8')

        columns.append(
            ColumnInfo(
                name=column_name,
                type=type,
                table_index=len(columns),  # Use the current length as the index
                is_primary_key=False
            )
        )

    return TableSchema(
        table_name=table_name,
        columns=columns
    )

def parse_create_table(sql: str) -> TableSchema:
    sql_re = rewrite_sql_for_parsing(sql)  # Rewrite the SQL to fix some parsing issues
    try:
        try:
            return parse_create_table_with_sqloxide(sql_re)
        except Exception as e:
            return parse_with_simple_ddl_parser(sql_re)
    except Exception as e:
        return parse_create_table_with_tree_sitter(sql)




def parse_create_table_with_sqloxide(sql: str) -> TableSchema:
    """
    Parses a CREATE TABLE SQL statement to extract column metadata.

    Args:
        sql (str): The CREATE TABLE statement.

    Returns:
        TableSchema: A TableSchema object representing the table's structure.
    """
    output = parse_sql(sql=sql, dialect='generic')
    SEARCHED_OP = 'CreateTable'

    if not output[0][SEARCHED_OP]:
        raise ValueError("The provided SQL statement is not a valid CREATE TABLE statement.")

    # the table name also included the schema, so we need to extract it
    create_table_statement = output[0][SEARCHED_OP]
    identifier_values = [e['Identifier']['value'] for e in create_table_statement['name']]
    table_name = '.'.join(identifier_values).lower() if identifier_values else None
    statement_cols = create_table_statement['columns']

    columns: List[ColumnInfo] = []
    for (index, statement_col) in enumerate(statement_cols):
        data_type = get_data_type(statement_col)
        name = statement_col['name']['value']

        is_primary_key = False
        for option in statement_col.get('options', []):
            internal_option = option['option']
            if 'Unique' in internal_option:
                if 'is_primary' in internal_option['Unique']:
                    is_primary_key = internal_option['Unique']['is_primary']

        columns.append(
            ColumnInfo(
                name=name,
                table_index=index,
                type=data_type,
                is_primary_key=is_primary_key
            )
        )
        pass

    if table_name is None:
        raise ValueError("Table name not found in the provided CREATE TABLE query.")

    return TableSchema(table_name=table_name, columns=columns)


if __name__ == "__main__":
    test_parse_create_scheduled_games_table()
