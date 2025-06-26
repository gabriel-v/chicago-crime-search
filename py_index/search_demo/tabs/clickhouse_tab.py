from dash import html, dcc, callback, Output, Input, State, ALL, no_update
import pandas as pd
from clickhouse_connect import get_client
from py_index.database_settings import CLICKHOUSE_SETTINGS
from py_index.search_demo.components import create_data_table, create_error_div
import time
import datetime
import traceback

def create_clickhouse_tab():
    return html.Div([
        # Top row with 50/50 split
        html.Div([
            # Left side - Query input
            html.Div([
                dcc.Textarea(
                    id='clickhouse-query',
                    placeholder='Enter your Clickhouse SQL query here...',
                    style={'width': '100%', 'height': 200},
                    persistence=True,
                    persistence_type='local'
                ),
                html.Button('Submit Query', id='clickhouse-submit', n_clicks=0),
            ], style={'padding': '20px', 'width': '50%', 'float': 'left'}),
            
            # Right side - Query history
            html.Div([
                html.Div(id='clickhouse-history', style={'padding': '20px'})
            ], style={'width': '50%', 'float': 'right'})
        ], style={'display': 'flex', 'clear': 'both'}),
        
        # Bottom row - Query results
        html.Div(id='clickhouse-output', style={'padding': '20px', 'clear': 'both'})
    ])

def get_history_buttons():
    """Helper function to get history buttons"""
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        df = client.query_df("""
            SELECT 
                event_time,
                query,
                result_time_ms,
                result_summary
            FROM search_demo_query_history h INNER JOIN (SELECT query, max(event_time) as event_time from search_demo_query_history GROUP BY query) d ON d.query = h.query and d.event_time = h.event_time
            WHERE item_type = 'clickhouse'
            ORDER BY event_time DESC
            LIMIT 10
        """)
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
    Output('clickhouse-query', 'value'),
    Input({'type': 'history-item', 'query': ALL}, 'n_clicks'),
    State({'type': 'history-item', 'query': ALL}, 'id'),
    State({'type': 'history-item', 'query': ALL}, 'n_clicks_timestamp'),
    prevent_initial_call=True
)
def set_query_from_history(n_clicks, ids, timestamps):
    if not any(n_clicks):
        return no_update
        
    # Find which button was clicked most recently using timestamps
    clicked_idx = timestamps.index(max(t for t, n in zip(timestamps, n_clicks) if n))
    print('set query from history', clicked_idx, ids[clicked_idx]['query'], 'timestamp:', timestamps[clicked_idx])
    return ids[clicked_idx]['query']


@callback(
    Output('clickhouse-history', 'children'),
    Input('clickhouse-submit', 'n_clicks'),
)
def update_history(n_clicks):
    buttons = get_history_buttons()
    print("refresh history clicks: ", n_clicks, "button count: ", len(buttons))
    return buttons

@callback(
    Output('clickhouse-output', 'children'),
    Input('clickhouse-submit', 'n_clicks'),
    State('clickhouse-query', 'value'),
    prevent_initial_call=True
)
def run_clickhouse_query(n_clicks, query):
    if not query or not query.strip():
        return html.Div("Please enter a query", style={'color': 'red'})
    
    print("run query")
    try:
        with get_client(**CLICKHOUSE_SETTINGS) as client:
            t0 = time.time()
            result = client.query_df(query)
            dt_ms = (time.time() - t0) * 1000
            
            # Store query history
            client.insert(
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
                    'clickhouse',
                    query,
                    dt_ms,
                    f"{len(result)} rows ({dt_ms:.2f}ms)"
                ]])
            
            return create_data_table(
                result, 
                title=f'Clickhouse Query Results - {len(result)} rows ({dt_ms:.2f}ms)'
            )
    except Exception as e:
        return create_error_div(e)