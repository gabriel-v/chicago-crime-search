import time
from clickhouse_connect import get_client
from py_index.database_settings import CLICKHOUSE_SETTINGS
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


def execute_query(client, query):
    start_time = time.time()
    print('SQL> ', query)
    result = client.command(query)
    print('SQL< returned', result.as_query_result().row_count, 'rows')
    end_time = time.time()
    print(f"dt = {end_time - start_time:.2f} seconds")
    print()
    return result

def reset_database():
    with get_client(host='localhost', user='chicago_crimes_search', password='chicago_crimes_search') as client:
        # Drop and create database
        execute_query(client, 'DROP DATABASE IF EXISTS chicago_crimes_search SYNC;')
        execute_query(client, 'CREATE DATABASE chicago_crimes_search;')
        execute_query(client, 'USE chicago_crimes_search;')


def reset_tables():
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        execute_query(client, 'DROP TABLE IF EXISTS input_tables_list SYNC;')
        execute_query(client, '''CREATE TABLE input_tables_list (
            table_name String,
            file_name String,
            item_name String,
            event_time DateTime,
            file_size UInt64,
        ) ENGINE = MergeTree() ORDER BY (table_name)
        ''')

        execute_query(client, 'DROP TABLE IF EXISTS input_tables_raw_columns SYNC;')
        execute_query(client, '''
        CREATE TABLE input_tables_raw_columns (
            table_name String,
            column_index UInt64,
            column_name String,
            column_type String,
            column_base_type String,
            column_null_count UInt64,
            column_non_null_count UInt64,
            column_unique_count UInt64,
            column_null_percentage Float64,
            column_unique_percentage Float64,
            column_name_fixed String
        ) ENGINE = ReplacingMergeTree() ORDER BY (table_name, column_index)
        ''')

        execute_query(client, 'DROP TABLE IF EXISTS input_tables_final_columns SYNC;')
        execute_query(client, '''
        CREATE TABLE input_tables_recreated (
            table_name String,
            original_table_name String,
        ) ENGINE = MergeTree() ORDER BY (table_name)
        ''')


        execute_query(client, '''
        CREATE TABLE IF NOT EXISTS index_status_event (
            table_name String,
            event_time DateTime,
            status String
        ) ENGINE = MergeTree() ORDER BY (table_name, event_time);
        ''')

        execute_query(client, '''
        CREATE TABLE IF NOT EXISTS input_indexing_done (
            table_name String,
            event_time DateTime,
        ) ENGINE = MergeTree() ORDER BY table_name;
        ''')

        execute_query(client, """
            CREATE OR REPLACE VIEW input_tables_summary
            AS SELECT
                i.file_size,
                i.file_name,
                i.item_name,
                r.table_name AS table_name,
                i.event_time AS indexing_started_at,
                d.event_time AS indexing_finished_at,
                dateDiff('s', i.event_time, d.event_time) AS index_duration_s,
                (i.file_size / index_duration_s) / 1024. AS index_speed_kbps
            FROM input_tables_list AS i
            INNER JOIN input_tables_recreated AS r ON r.original_table_name = i.table_name
            INNER JOIN input_indexing_done AS d ON d.table_name = r.table_name
            ORDER BY (file_size, file_name) ASC
        """)

        execute_query(client, """DROP TABLE IF EXISTS search_demo_query_history SYNC;""")
        execute_query(client, """
        CREATE TABLE IF NOT EXISTS search_demo_query_history (
            event_time DateTime,
            item_type String,
            query String,
            result_time_ms Float64,
            result_summary String
        ) ENGINE = MergeTree() ORDER BY (item_type, event_time DESC)
                      SETTINGS allow_experimental_reverse_key = 1
        """)



def fetch_table_raw_column_stats(table_name):
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        table_row_count = client.query(f'''
        SELECT count()
        FROM {table_name}
        ''').result_rows[0][0]

        print("\n================================================")
        print(f"Processing table: `{table_name}` with {table_row_count} rows")
        print()
        if table_row_count == 0:
            table_row_count = 1

        # Get columns for the table
        result = client.query(f'''
        SELECT name , type
        FROM system.columns
        WHERE table = '{table_name}'
        AND database = 'chicago_crimes_search'
        ''')
    # Process each column in parallel
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for column_index, [column_name, column_type] in enumerate(result.result_rows):
            future = executor.submit(_fetch_raw_column_stats, table_name, column_index, column_name, column_type, table_row_count)
            futures.append(future)

        # Wait for all tasks to complete and handle any exceptions
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing column: {str(e)}")
    return True


def _fetch_raw_column_stats(table_name, column_index, column_name, column_type, table_row_count):
    print(f"Loading column stats for {table_name}.{column_name}")
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        column_base_type = re.match(r'^(?:Nullable\()?([^()]+)(?:\))?$', column_type).group(1)

        # Get null count
        null_count_result = client.query(f'''
        SELECT count()
        FROM `{table_name}`
        WHERE `{column_name}` IS NULL
        ''')
        null_count = null_count_result.result_rows[0][0]
        non_null_count = table_row_count - null_count
        null_percentage = 100.0 * null_count / (table_row_count )

        # Get unique count
        unique_count_result = client.query(f'''
        SELECT count(DISTINCT `{column_name}`)
        FROM `{table_name}`
        ''')
        unique_count = unique_count_result.result_rows[0][0]
        if non_null_count == 0 or unique_count == 0:
            unique_percentage = 0
        else:
            unique_percentage = 100.0 * unique_count / (non_null_count )

        # replace non-alphanumeric characters with underscore. also turn lower case.
        column_name_fixed = re.sub(r'[^a-zA-Z0-9]', ' ', column_name).lower().strip().replace('  ', ' ').replace(' ', '_')[:50]
        column_name_fixed = f'c{str(column_index).zfill(3)}_{column_name_fixed}'

        # Insert results
        client.insert(
            'input_tables_raw_columns',
            column_names = [
                'table_name',
                'column_index',
                'column_name',
                'column_type',
                'column_base_type',
                'column_null_count',
                'column_non_null_count',
                'column_unique_count',
                'column_null_percentage',
                'column_unique_percentage',
                'column_name_fixed'
            ],
            data = [[
                table_name,
                column_index,
                column_name,
                column_type,
                column_base_type,
                null_count,
                non_null_count,
                unique_count,
                null_percentage,
                unique_percentage,
                column_name_fixed
            ]])


def recreate_table(table_name):
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        item = client.query(f"select item_name, table_name from input_tables_list where table_name = '{table_name}'")
        if item.result_rows:
            item_name = item.result_rows[0][0]
            original_table_name = item.result_rows[0][1]
        else:
            print(f"Table {table_name} not found in input_tables_list")
            return
        new_table_name = f"table_{item_name}"
        columns = client.query_df(f"select * from input_tables_raw_columns where table_name = '{table_name}' order by column_index").to_dict(orient='records')

        try:
            return _recreate_table_impl(client, original_table_name, columns, new_table_name)
        except Exception as e:
            print(f"Error recreating table {table_name}: {str(e)}")
            return None

def _recreate_table_impl(client, original_table_name, columns, new_table_name):
        # print("old table", original_table_name, "columns", columns)
    create_columns = ",\n\t".join(_create_column_sql(column) for column in columns)
    select_columns = ",\n\t".join(f"`{column['column_name']}` AS `{column['column_name_fixed']}`" for column in columns)

    client.command(f"DROP TABLE IF EXISTS {new_table_name} SYNC;")
    create_sql = f"""
    CREATE TABLE {new_table_name} (
        `id` Int64 ,
        {create_columns}
    ) ENGINE = MergeTree() SAMPLE BY intHash32(id) ORDER BY (id, intHash32(id))
    AS
        SELECT 1+toInt64(generateSerialID('{new_table_name}')) as id,
        {select_columns}
    FROM {original_table_name}
    """
    execute_query(client, create_sql)
    optimize_sql = f"OPTIMIZE TABLE {new_table_name} FINAL;"
    execute_query(client, optimize_sql)
    execute_query(client, f"DROP TABLE IF EXISTS {original_table_name} SYNC;")
    execute_query(client, f"""
        INSERT INTO input_tables_recreated (table_name, original_table_name)
        VALUES ('{new_table_name}', '{original_table_name}')
    """)
    return new_table_name


def _create_column_sql(column_stats):
    print(column_stats)
    if column_stats['column_null_percentage'] == 0:
        column_type = column_stats['column_base_type']
    else:
        column_type = column_stats['column_type']
    if column_type == 'String' and column_stats['column_unique_count'] < 1000 and column_stats['column_unique_percentage'] < 10 and column_stats['column_non_null_count'] > 1000:
        column_type = 'LowCardinality(String)'
    column_name = column_stats['column_name_fixed']
    return f"`{column_name}` {column_type}"