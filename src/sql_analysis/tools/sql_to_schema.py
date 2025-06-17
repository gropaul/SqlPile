from dataclasses import dataclass
from typing import List

from sqloxide import parse_sql

from src.sql_analysis.tools.parse_sql import print_recursive


@dataclass
class ColumnInfo:
    """Represents column metadata."""
    name: str
    type: str
    is_primary_key: bool = False


@dataclass
class TableSchema:
    """Represents the schema of a table."""
    table_name: str
    columns: List[ColumnInfo]


categories_table = """
create table `categories` ( `id` bigint ( 20 ) unsigned not null , `name` varchar ( 256 ) not null default 'unknown' , `disabled` tinyint ( 1 ) unsigned not null default 0 , `weight` decimal ( 3 , 2 ) unsigned not null default 1.00 , `selection_count` bigint ( 20 ) unsigned default 0 comment 'number of times this category has been chosen to be played on its own ( regardless of if it was allowed ) ' , `questions_asked` bigint ( 20 ) unsigned default 0 , `translation_dirty` tinyint ( 1 ) unsigned default 0 , `trans_tr` text default null , `trans_pt` text default null , `trans_fr` text default null , `trans_es` text default null , `trans_de` text default null , `trans_hi` text default null , `trans_sv` text default null , `trans_ru` text default null , `trans_pl` text default null , `trans_it` text default null , `trans_ja` text default null , `trans_ko` text default null , `trans_bg` text default null , `trans_zh` text default null , `trans_ar` text default null , `trans_nl` text default null ) engine = innodb default charset = utf8mb4
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

infobot_table = """
                create table `infobot_discord_counts`
                (
                    `shard_id`                  bigint(20) unsigned not null comment 'shard id',
                    `cluster_id`                int(10) unsigned    not null default 0,
                    `dev`                       tinyint(1) unsigned not null default 0 comment 'true if development data',
                    `user_count`                bigint(20)          not null,
                    `server_count`              bigint(20)          not null,
                    `shard_count`               bigint(20) unsigned not null default 1 comment 'number of shards',
                    `channel_count`             bigint(20) unsigned not null,
                    `sent_messages`             bigint(20) unsigned not null default 0,
                    `received_messages`         bigint(20) unsigned not null default 0,
                    `memory_usage`              bigint(20) unsigned not null default 0,
                    `games`                     bigint(20)          not null default 0 comment 'number of concurrent active games in progress',
                    `last_updated`              datetime            not null default current_timestamp() on update current_timestamp(),
                    `last_restart_intervention` datetime                     default null
                ) engine = innodb
                  default charset = latin1 comment = 'counts of users / servers on a per - shard basis' \
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


def test_parse_create_infobot_table():
    schema = parse_create_table(infobot_table)
    expected_column_names = [
        'shard_id', 'cluster_id', 'dev', 'user_count',
        'server_count', 'shard_count', 'channel_count',
        'sent_messages', 'received_messages', 'memory_usage',
        'games', 'last_updated', 'last_restart_intervention'
    ]

    print(schema)

    # print the missing columns
    missing_columns = [col for col in expected_column_names if col not in [c.name for c in schema.columns]]
    if missing_columns:
        print(f"Missing columns: {missing_columns}")

    assert schema.table_name == 'infobot_discord_counts'
    assert len(schema.columns) == len(expected_column_names)

    for column in schema.columns:
        assert column.name in expected_column_names
        assert isinstance(column.type, str)


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


def rewrite_sql_for_parsing(sql: str) -> str:
    # Match variations like:
    # decimal(5,2) unsigned
    # decimal (5 , 2 ) unsigned
    # decimal( 5 ,2)   unsigned
    pattern = re.compile(r'decimal\s*\(\s*\d+\s*,\s*\d+\s*\)\s*unsigned', re.IGNORECASE)

    # Replace with just the decimal definition, removing 'unsigned'
    def replacer(match):
        # Extract the decimal part without 'unsigned'
        decimal_part = re.search(r'decimal\s*\(\s*\d+\s*,\s*\d+\s*\)', match.group(0), re.IGNORECASE)
        return decimal_part.group(0) if decimal_part else match.group(0)

    sql = pattern.sub(replacer, sql)

    # the parser does not support the "zerofill" specifier, so we remove it
    sql = re.sub(r'\bzerofill\b', '', sql, flags=re.IGNORECASE)
    return sql


def parse_create_table(sql: str) -> TableSchema:
    """
    Parses a CREATE TABLE SQL statement to extract column metadata.

    Args:
        sql (str): The CREATE TABLE statement.

    Returns:
        TableSchema: A TableSchema object representing the table's structure.
    """
    sql = rewrite_sql_for_parsing(sql)  # Rewrite the SQL to fix some parsing issues
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
    for statement_col in statement_cols:
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
                type=data_type,
                is_primary_key=is_primary_key
            )
        )
        pass

    if table_name is None:
        raise ValueError("Table name not found in the provided CREATE TABLE query.")

    return TableSchema(table_name=table_name, columns=columns)


if __name__ == "__main__":
    test_parse_create_infobot_table()
    test_parse_create_scheduled_games_table()
