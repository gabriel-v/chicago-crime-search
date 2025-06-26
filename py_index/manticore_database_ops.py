#!/usr/bin/env python3

import os
import time
from clickhouse_connect import get_client
from py_index.database_settings import CLICKHOUSE_SETTINGS



def index_table_into_manticore(table_name):
    search_configs, folders = generate_configs()

    print(f"Writing manticore.conf - {len(search_configs)} bytes")
    with open('docker/manticore.conf', 'w') as f:
        f.write(search_configs)

    print("Updating manticore configs")
    import subprocess
    envs = os.environ.copy()
    envs['MSYS_NO_PATHCONV'] = '1'
    subprocess.check_call(['docker', 'exec', 'manticore', 'bash', '-c', f'mkdir -p {" ".join(folders)}'])
    subprocess.check_call(['docker', 'exec', 'manticore', 'bash', '/manticore-update-config.sh', table_name], env=envs)
    try:
        wait_until_manticore_table_is_ready(table_name)
        # connect_clickhouse_table_to_manticore_idx(table_name)
        with get_client(**CLICKHOUSE_SETTINGS) as client:
            client.command(f"INSERT INTO input_indexing_done (table_name, event_time) VALUES ('{table_name}', NOW())")
    except Exception as e:
        print(f"Error connecting table {table_name} to manticore: {str(e)}")
        return

def generate_configs():
    config_sections = []
    folders = []
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        tables = client.query_df('select table_name from input_tables_recreated')['table_name'].tolist()
        for table in tables:
            print(f"Generating config for table {table}")
            container_folder, config = table_config_section(client, table)
            config_sections.append(config)
            folders.append(container_folder)
    top_section = """
        searchd {
            listen = 0.0.0.0:9312
            listen = 0.0.0.0:9306:mysql
            listen = 0.0.0.0:9308:http
            log = /var/log/manticore/searchd.log
            query_log = /var/log/manticore/query.log
            pid_file = /var/run/manticore/searchd.pid
            # data_dir = /var/lib/manticore
        }
    """
    config_text = top_section + "\n".join(config_sections)
    print(config_text)
    return config_text, folders


def table_config_section(client, table_name):
    columns = client.query_df(f"select name, type from system.columns where table = '{table_name}'")
    column_select_sql = []
    extra_attribute_lines = []
    for column in columns.to_dict(orient='records'):
        convert_timestamp = False
        if column['name'] != 'id':
            if column['type'] == 'LowCardinality(String)':
                extra_attribute_lines.append(f"sql_field_string =  {column['name']}")
            if column['type'] in [ 'Int64', 'Nullable(Int64)']:
                extra_attribute_lines.append(f"sql_attr_bigint =  {column['name']}")
            if column['type'] in [ 'Float64', 'Nullable(Float64)']:
                extra_attribute_lines.append(f"sql_attr_float =  {column['name']}")
            if column['type'] in [ 'DateTime', 'Nullable(DateTime)', 'Date', 'Nullable(Date)']:
                convert_timestamp = True
                extra_attribute_lines.append(f"sql_attr_timestamp =  {column['name']}")
        if convert_timestamp:
            column_select_sql.append(f"toUnixTimestamp ({column['name']}) as {column['name']}")
        else:
            column_select_sql.append(f"{column['name']}")
    column_list_str = ", ".join(column_select_sql)
    sql_query = f"SELECT {column_list_str} FROM {table_name}"
    extra_attribute_lines = "\n".join(extra_attribute_lines)

    container_folder = f"/var/lib/manticore/v1/{table_name}"
    table_config = f"""

    table {table_name} {{
        type = plain
        path = {container_folder}/data
        source = {table_name}
        columnar_attrs = *
        min_infix_len = 3

    }}
    source {table_name} {{
        type =  mysql

        sql_host = clickhouse
        sql_port = 9004
        sql_user = {CLICKHOUSE_SETTINGS['user']}
        sql_pass = {CLICKHOUSE_SETTINGS['password']}
        sql_db = {CLICKHOUSE_SETTINGS['database']}

        sql_query_pre    = SET CHARACTER_SET_RESULTS=utf8
        sql_query_pre    = SET NAMES utf8
        sql_query_pre    = INSERT INTO index_status_event (table_name, event_time, status) VALUES ('{table_name}', NOW(), 'started');
        sql_query_post = INSERT INTO index_status_event (table_name, event_time, status) VALUES ('{table_name}', NOW(), 'query_ended');
        sql_query_post_index = INSERT INTO index_status_event (table_name, event_time, status) VALUES ('{table_name}', NOW(), 'done');

        sql_query = {sql_query}

        {extra_attribute_lines}
    }}
    """
    return (container_folder, table_config)


def connect_clickhouse_table_to_manticore_idx(table):
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        # Get both name and type columns from system.columns
        column_df = client.query_df(f"select name, type from system.columns where table = '{table}'")
        if column_df.empty:
            print(f"Table {table} not found")
            return
        # Apply the conversion to each type
        column_df['mysql_type'] = column_df['type'].apply(convert_clickhouse_type_to_manticore_mysql)
        # Create the column definitions for the CREATE TABLE statement
        column_defs = [f"{row['name']} {row['mysql_type']}" for _, row in column_df.iterrows()]
        column_list_str = ", ".join(column_defs)

        print(f"Binding table {table}")
        client.command(f"DROP TABLE IF EXISTS search_{table} SYNC;")
        query = f"""
            CREATE TABLE IF NOT EXISTS search_{table} ({column_list_str})
            ENGINE = MySQL('manticore:9306', 'manticore', '{table}', 'user', 'pass')
        """
        print(query)
        client.command(query)
        return f"search_{table}"


def convert_clickhouse_type_to_manticore_mysql(clickhouse_type):
    if clickhouse_type.startswith('Nullable'):
        null = True
        clickhouse_type = clickhouse_type.split('(')[1].split(')')[0]
    else:
        null = False
    my_type = 'TEXT'
    if clickhouse_type == 'Bool':
        my_type = 'BOOL'
    if clickhouse_type == 'Int64':
        my_type = 'BIGINT'
    if clickhouse_type == 'Float64':
        my_type = 'FLOAT'
    if clickhouse_type in ['DateTime', 'Date']:
        my_type = 'BIGINT'  # manticore converts all dates to unix timestamp
    if null:
        my_type = my_type + ' NULL'
    else :
        my_type = my_type + ' NOT NULL'
    return my_type


def manticore_client_data_server():
    import pymysql
    return pymysql.connect(
        host='localhost',
        port=9306,
        user='user',
        password='pass',
        # database='manticore'
    )
    # this does not work because manticore does not support sqlalchemy
    # (it returns int for get_isolation_leve() where something wanted string)
    # import sqlalchemy
    # return sqlalchemy.create_engine(f'mysql+pymysql://manticore:manticore@localhost:9306/manticore')


def manticore_client_weights_server():
    import pymysql
    return pymysql.connect(
        host='localhost',
        port=19306,
        user='user',
        password='pass',
    )


def manticore_query(client, query, args=None):
    import pandas as pd
    print('manticore query: ', query[:160])
    with client.cursor() as cursor:
        cursor.execute(query, args)

        # Get all result sets
        result_sets = []
        while True:
            # For non-SELECT queries (INSERT, UPDATE, etc), description will be None
            if cursor.description is None:
                # For non-SELECT queries, just return None
                return None

            column_names = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=column_names)
            result_sets.append(df)

            # Try to get next result set
            try:
                has_more = cursor.nextset()
                if not has_more:
                    break
            except:
                break

        # Return single DataFrame if only one result set, otherwise return list
        if len(result_sets) == 1:
            return result_sets[0]
        return result_sets


def manticore_executemany(client, query, args_list):
    print('manticore executemany: ', query[:160], ' - ', len(args_list), ' args')
    with client.cursor() as cursor:
        cursor.executemany(query, args_list)


def wait_until_manticore_table_is_ready(table_name):
    print('wait until manticore table is ready')

    for i in range(10):
        try:
            with manticore_client_data_server() as client:
                manticore_query(client, f"SELECT COUNT(*) as count FROM {table_name}")['count'].iloc[0]
                manticore_query(client, f"SELECT * FROM {table_name} LIMIT 1")
                manticore_query(client, f"CALL AUTOCOMPLETE('the', '{table_name}')")
                print('manticore table OK')
                return True
        except Exception as e:
            print(f"Error waiting for manticore table {table_name} {i}/10:\n {str(e)}")
        time.sleep(i + 1)
    return False

