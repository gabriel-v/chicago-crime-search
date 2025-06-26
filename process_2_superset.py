import requests
import clickhouse_connect
from py_index.database_settings import CLICKHOUSE_SETTINGS
import json
SUPERSET_URL = "http://localhost:8088"
SUPERSET_DBNAME = "chicago_crime_search"

import uuid

def get_superset_client():
    print("Logging in to Superset")
    s = requests.Session()
    r = s.post(SUPERSET_URL + "/api/v1/security/login", json={
        "username": "admin",
        "password": "admin",
        "refresh": False,
        'provider': 'db'
    }).json()
    print(r)
    access_token = r['access_token']
    print("Successfully logged in to Superset")
    s.headers.update({
        "Authorization": f"Bearer {access_token}"
    })
    print("superset LOGIN OK")
    return s

def connect_superset_clickhouse_database(s):
    print("Connecting Superset to clickhouse database")
    URL = SUPERSET_URL + "/api/v1/database/"
    item_list = s.get(URL).json()
    item_list = item_list['result']
    try:
        next_id = max([i['id'] for i in item_list]) + 1
    except Exception as e:
        print(f"Error getting next id: {repr(e)}")
        next_id = 1
    valid_items = [i for i in item_list
                if i['backend'] == 'clickhousedb'
                and i['database_name'] == SUPERSET_DBNAME]
    if len(valid_items) > 0:
        print("Clickhouse database already connected to Superset")
        return valid_items[0]['id']

    # Create a new clickhouse database
    csrf = s.get(SUPERSET_URL + "/api/v1/security/csrf_token").json()['result']
    print('csrf', csrf)
    r = s.post(URL, json={
        "database_name": SUPERSET_DBNAME,
        "id": next_id,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_file_upload": False,
        "allow_run_async": False,
        "allow_cost_estimate": False,
        # "configuration_method": "sqlalchemy_uri",
        # "driver": "string",
        # "engine": "string",
        "expose_in_sqllab": True,
        "extra": '{"allows_virtual_table_explore":true}',
        # "force_ctas_schema": "string",
        "impersonate_user": False,
        "is_managed_externally": True,
        # "masked_encrypted_extra": "string",
        # "parameters": {
        #     "additionalProp1": "string",
        #     "additionalProp2": "string",
        #     "additionalProp3": "string"
        # },
        # "server_cert": "string",
        "sqlalchemy_uri": "clickhousedb://chicago_crimes_search:chicago_crimes_search@clickhouse:8123/chicago_crimes_search",
        "uuid": str(uuid.uuid4())
    }, headers={'X-CSRFToken': csrf})
    print(r.json())
    r.raise_for_status()
    print("Clickhouse database successfully connected to Superset")
    return r.json()['id']

def create_superset_table(s, superset_db_id, table_name):
    print(f"Creating Superset table {table_name}")
    try:
        csrf = s.get(SUPERSET_URL + "/api/v1/security/csrf_token").json()['result']
        url = SUPERSET_URL + f"/api/v1/dataset/"
        r = s.post(url, json={
            "database": superset_db_id,
            "schema": "chicago_crimes_search",
            "table_name": table_name,
            "catalog": None,
        }, headers={'X-CSRFToken': csrf})
        r.raise_for_status()
        r = r.json()
        table_id = r['id']
        table_data = json.dumps(r['data'], indent=2)
        with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as c:
            c.insert('superset_tables', [[superset_db_id, table_id, table_name, table_data]], column_names=[
                'superset_database_id',
                'superset_table_id',
                'table_name',
                'superset_table_info'
            ])
    except Exception as e:
        print(f"Error creating Superset table: {table_name}: {repr(e)}")

def create_superset_tables(s, superset_db_id):
    with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as c:
        tables_that_exist = c.query_df("SELECT table_name from superset_tables")
        if len(tables_that_exist) == 0:
            tables_that_exist = set()
        else:
            tables_that_exist = set(tables_that_exist['table_name'].tolist())
        tables_to_create =  c.query_df("SELECT table_name FROM input_tables_summary")
        if len(tables_to_create) == 0:
            print("No tables to create")
            return
        tables_to_create = [t for t in tables_to_create['table_name'].tolist() if t not in tables_that_exist]
    print(f"Creating {len(tables_to_create)} Superset tables")
    for table in tables_to_create:
        create_superset_table(s, superset_db_id, table)

def create_superset_charts_all_tables(s):
    with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as c:
        charts_to_create = c.query_df("SELECT * FROM superset_tables")
    if len(charts_to_create) == 0:
        print("No charts to create")
        return
    print(f"Creating {len(charts_to_create)} Superset charts")
    charts_to_create = charts_to_create.to_dict(orient='records')
    for table in charts_to_create:
        create_superset_chart_for_table(s, table)


def create_superset_chart_for_table(s, table):
    table_name = table['table_name']
    superset_db_id = table['superset_database_id']
    superset_table_id = table['superset_table_id']
    # _superset_table_info = table['superset_table_info']

    chart_name = f"{table_name}_chart"
    with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as c:
        columns = c.query_df(f"SELECT name, type FROM system.columns WHERE table = '{table_name}' and database = 'chicago_crimes_search'").to_dict(orient='records')

    for column in columns:
        if column['type'] == 'LowCardinality(String)':
            # crate word cloud chart for this column
            chart_name = f"word cloud / {column['name']} / {table_name}"
            create_superset_chart_for_column(
                s, superset_table_id, table_name, column['name'], chart_name, 'word_cloud')

def create_superset_chart_for_column(
        s, superset_table_id, table_name, column_name, chart_name, chart_type):
    print(f"Creating Superset chart for {table_name}")
    params = f'''
    {{
        "datasource":"{superset_table_id}__table",
        "viz_type":"{chart_type}",
        "series":"{column_name}",
        "metric":"count",
        "adhoc_filters":[],
        "row_limit":100,
        "size_from":10,
        "size_to":70,
        "rotation":"square",
        "color_scheme":"supersetColors",
        "extra_form_data":{{}},
        "dashboards":[]
    }}
    '''
    query_context = f'''

    {{
        "datasource":{{"id":{superset_table_id},"type":"table"}},
        "force":false,
        "queries":[
        {{
            "filters":[],
            "extras":{{"having":"","where":""}},
            "applied_time_extras":{{}},
            "columns":["{column_name}"],
            "metrics":["count"],
            "annotation_layers":[],
            "row_limit":100,
            "series_limit":0,
            "order_desc":true,
            "url_params":{{}},
            "custom_params":{{}},
            "custom_form_data":{{}}
        }}
        ],
        "form_data":  {{
            "datasource":"{superset_table_id}__table",
            "viz_type": "{chart_type}",
            "series":"{column_name}",
            "metric":"count",
            "adhoc_filters":[],
            "row_limit":100,
            "size_from":10,
            "size_to":70,
            "rotation":"square",
            "color_scheme":"supersetColors",
            "extra_form_data":{{}},
            "dashboards":[],
            "force":false,
            "result_format":"json",
            "result_type":"full"
        }},
        "result_format":"json","result_type":"full"
    }}
    '''
    create_chart_data = {
        "dashboards": [],
        "datasource_id": superset_table_id,
        "datasource_type": "table",
        "is_managed_externally": False,
        "owners": [ ],
        "slice_name": chart_name,
        "viz_type": chart_type,


        "params":	params,
        "query_context":	query_context

    }
    print("CREATE CHART:", create_chart_data)
    try:
        csrf = s.get(SUPERSET_URL + "/api/v1/security/csrf_token").json()['result']
        url = SUPERSET_URL + f"/api/v1/chart/"
        r = s.post(url, json=create_chart_data, headers={'X-CSRFToken': csrf})
        r.raise_for_status()
        r = r.json()
        chart_id = r['id']
        chart_data = json.dumps(r['result'], indent=2)
        with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as c:
            c.insert(
                'superset_charts',
                [[superset_table_id, table_name, chart_id, chart_name, chart_data]],
                column_names=[
                    'superset_table_id',
                    'table_name',
                    'superset_chart_id',
                    'superset_chart_name',
                    'superset_chart_data'
                ]
            )
    except Exception as e:
        print(f"Error creating Superset chart for {table_name}: {repr(e)}")
        raise e


def init_clickhouse_tables_about_superset():
    print("Initializing clickhouse tables about superset")
    with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as c:
        c.command("""CREATE TABLE IF NOT EXISTS superset_tables (
            superset_database_id UInt64,
            superset_table_id UInt64,
            table_name String,
            superset_table_info String
        ) ENGINE = MergeTree()
        ORDER BY (superset_database_id, superset_table_id, table_name)
        """)
        c.command("""CREATE TABLE IF NOT EXISTS superset_charts (
            superset_table_id UInt64,
            table_name String,
            superset_chart_id UInt64,
            superset_chart_name String,
            superset_chart_data String
        ) ENGINE = MergeTree()
        ORDER BY (superset_table_id, table_name, superset_chart_id)
        """)

def process_2_superset():
    init_clickhouse_tables_about_superset()
    print("Processing 2 superset")
    with get_superset_client() as s:
        superset_db_id = connect_superset_clickhouse_database(s)
        print("superset_db_id", superset_db_id)
        create_superset_tables(s, superset_db_id)

        create_superset_charts_all_tables(s)


if __name__ == "__main__":
    process_2_superset()