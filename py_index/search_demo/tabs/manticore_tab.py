from dash import html, dcc, callback, Output, Input, State, ALL, no_update
from py_index.manticore_database_ops import manticore_client_data_server, manticore_client_weights_server, manticore_query
from py_index.search_demo.components import create_data_table, create_error_div
from clickhouse_connect import get_client
from py_index.database_settings import CLICKHOUSE_SETTINGS
import time
import datetime
import traceback
import pandas as pd

def create_manticore_tab():
    return html.Div([
        # Top row with 50/50 split
        html.Div([
            # Left side - Query input
            html.Div([
                dcc.Textarea(
                    id='manticore-query',
                    placeholder='Enter your Manticore search query here...',
                    style={'width': '100%', 'height': 200},
                    persistence=True,
                    persistence_type='local'
                ),
                # Client selector dropdown
                dcc.Dropdown(
                    id='manticore-client-selector',
                    options=[
                        {'label': 'Manticore Data Server (9306)', 'value': 'data'},
                        {'label': 'Manticore Weights Server (19306)', 'value': 'weights'}
                    ],
                    value='data',
                    style={'width': '100%', 'marginTop': '10px', 'marginBottom': '10px'},
                    persistence=True,
                    persistence_type='local'
                ),
                html.Button('Submit Query', id='manticore-submit', n_clicks=0),
            ], style={'padding': '20px', 'width': '50%', 'float': 'left'}),
            
            # Right side - Query history
            html.Div([
                html.Div(id='manticore-history', style={'padding': '20px'})
            ], style={'width': '50%', 'float': 'right'})
        ], style={'display': 'flex', 'clear': 'both'}),
        
        # Bottom row - Query results
        html.Div(id='manticore-output', style={'padding': '20px', 'clear': 'both'})
    ])

def get_history_buttons(client_type='data'):
    """Helper function to get history buttons"""
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        df = client.query_df("""
            SELECT 
                event_time,
                query,
                result_time_ms,
                result_summary,
                item_type
            FROM search_demo_query_history h INNER JOIN (SELECT query, max(event_time) as event_time from search_demo_query_history GROUP BY query) d ON d.query = h.query and d.event_time = h.event_time
            WHERE item_type = %(item_type)s
            ORDER BY event_time DESC
            LIMIT 10
        """, parameters={'item_type': f'manticore_{client_type}'})
        print('history rows: ', len(df))
        
        # Convert each row into a button that sets the query
        def create_history_button(row):
            txt = row['query'].strip().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('   ', ' ').replace('  ', ' ')[:120]
            return html.Button(
                f"{row['event_time']} - {row['result_summary']} - {txt}", 
                id={'type': 'history-item', 'query': row['query']},
                style={'width': '100%', 'margin': '5px 0', 'textAlign': 'left'}
            )
        
        return [create_history_button(row) for _, row in df.iterrows()]

@callback(
    Output('manticore-history', 'children'),
    [Input('manticore-submit', 'n_clicks'),
     Input('manticore-client-selector', 'value')]
)
def update_history(n_clicks, client_type):
    buttons = get_history_buttons(client_type)
    print("refresh history clicks: ", n_clicks, "button count: ", len(buttons))
    return buttons

@callback(
    Output('manticore-query', 'value'),
    Input({'type': 'history-item', 'query': ALL}, 'n_clicks'),
    State({'type': 'history-item', 'query': ALL}, 'id'),
    State({'type': 'history-item', 'query': ALL}, 'n_clicks_timestamp'),
    prevent_initial_call=True
)
def set_query_from_history(n_clicks, ids, timestamps):
    if not any(n_clicks):
        return no_update
    print('set query from history')
        
    # Find which button was clicked most recently using timestamps
    clicked_idx = timestamps.index(max(t for t, n in zip(timestamps, n_clicks) if n))
    print('set query from history', clicked_idx, ids[clicked_idx]['query'], 'timestamp:', timestamps[clicked_idx])
    return ids[clicked_idx]['query']

@callback(
    Output('manticore-output', 'children'),
    Input('manticore-submit', 'n_clicks'),
    State('manticore-query', 'value'),
    State('manticore-client-selector', 'value'),
    prevent_initial_call=True
)
def run_manticore_query(n_clicks, query, client_type):
    if not query or not query.strip():
        return html.Div("Please enter a query", style={'color': 'red'})
    
    print("run query")
    try:
        # Select the appropriate client based on dropdown value
        client_func = manticore_client_data_server if client_type == 'data' else manticore_client_weights_server
        
        with client_func() as client:
            t0 = time.time()
            result = manticore_query(client, query)
            dt_ms = (time.time() - t0) * 1000

            # Handle multiple result sets vs single result
            if isinstance(result, list):
                tables = [
                    create_data_table(
                        df if df is not None else pd.DataFrame(), 
                        title=f'Result Set {i+1}'
                    ) for i, df in enumerate(result)
                ]
                output = html.Div([
                    html.H3(f'Query completed in {dt_ms:.2f}ms - Returned {len(result)} result sets'),
                    *tables
                ])
            else:
                # Handle single result (could be None, empty DataFrame, or DataFrame with data)
                df = result if result is not None else pd.DataFrame()
                output = html.Div([
                    html.H3(f'Query completed in {dt_ms:.2f}ms'),
                    create_data_table(
                        df,
                        title='Manticore Query Results'
                    )
                ])
            
        # Store query history in Clickhouse
        with get_client(**CLICKHOUSE_SETTINGS) as ch_client:
            ch_client.insert(
                'search_demo_query_history',
                column_names = [
                    'event_time',
                    'item_type',
                    'query',
                    'result_time_ms',
                    'result_summary'
                ],
                data = [[
                    datetime.datetime.now(),
                    f'manticore_{client_type}',
                    query,
                    dt_ms,
                    f"Query completed in {dt_ms:.2f}ms"
                ]])
                
        return output
    except Exception as e:
        return create_error_div(e) 