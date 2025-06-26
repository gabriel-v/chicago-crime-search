from dash import html, dcc, callback, Output, Input
from py_index.database_settings import CLICKHOUSE_SETTINGS
from clickhouse_connect import get_client
from py_index.manticore_database_ops import manticore_client_data_server, manticore_query
from py_index.search_demo.components import create_data_table, create_error_div
import pandas as pd
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from functools import partial

def create_manticore_autocomplete_tab():
    return html.Div([
        # Single line text input
        dcc.Input(
            id='manticore-autocomplete-input',
            type='text',
            placeholder='Type to see live updates...',
            style={'width': '95%', 'padding': '10px', 'marginBottom': '20px'},
            persistence=True,
            persistence_type='local',
            debounce=0.25
        ),
        
        # Live output area
        html.Div(id='manticore-autocomplete-output')
    ])

@callback(
    Output('manticore-autocomplete-output', 'children'),
    Input('manticore-autocomplete-input', 'value')
)
def update_output(value):
    try:
        if value is None:
            return '...'
        if value.strip() == '':
            return '...'
        if len(value) < 3:
            return '... 3 letters plz ...'
        
        with get_client(**CLICKHOUSE_SETTINGS) as client:
            tables = client.query_df('select table_name from input_tables_summary')['table_name'].tolist()
        
        t0 = time.time()
        data = list(autocomplete_query_all_tables(tables, value))
        dt_ms = (time.time() - t0) * 1000
        short_list = combine_autocomplete_results(data)
        
        # Convert to pandas DataFrame and format hits as strings
        df = pd.DataFrame(data, columns=['table', 'hits'])
        df['hits'] = df['hits'].apply(lambda x: ', '.join(x))  # Convert list to comma-separated string
        
        return [
            html.Pre(", ".join(short_list), style={'maxWidth': '95%', 'overflow': 'auto', 'whiteSpace': 'pre-wrap'}),
            create_data_table(df, title=f'Autocomplete Results for "{value}" on {len(tables)} tables ({dt_ms:.2f}ms)'),
            html.Div(f'{len(tables)} tables')
        ]
    except Exception as e:
        return create_error_div(e)

def autocomplete_query_all_tables(tables, query):
    # Use ThreadPool to parallelize queries
    with ThreadPoolExecutor(max_workers=6) as executor:
        # Create a partial function with the query parameter fixed
        query_fn = partial(autocomplete_query_table, query=query)
        # Map the function over tables and collect results as they complete
        future_to_table = {executor.submit(query_fn, table): table for table in tables}
        
        # Collect and yield results as they complete
        for future in future_to_table:
            table = future_to_table[future]
            try:
                hits = future.result()
                if hits:  # Only yield if we have hits
                    yield (table, hits)
            except Exception as e:
                print(f"Error querying table {table}: {str(e)}")

def autocomplete_query_table(table, query):
    sql = f"CALL AUTOCOMPLETE(%s, '{table}')"
    with manticore_client_data_server() as client:
        df = manticore_query(client, sql, (query,))
    if not df.empty:
        return df['query'].tolist()
    return []

def combine_autocomplete_results(data):
    data = data[::-1]
    values = [d[1] for d in data]
    present = set()
    final = []
    maxlen = max(len(v) for v in values) if values else 0
    for i in range(maxlen):
        for v in values:
            if i < len(v):
                if v[i] not in present:
                    present.add(v[i])
                    final.append(v[i])
                    if len(final) > 100:
                        return final
    return final
