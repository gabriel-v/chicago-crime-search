import clickhouse_connect
from dash import html, dcc, callback, Input, Output, State
from py_index.database_settings import CLICKHOUSE_SETTINGS
from py_index.search_demo.components import create_error_div
from py_index.manticore_database_ops import manticore_client_weights_server, manticore_query
import process_3_encode_sentence as process_3
import time
import pandas as pd
from collections import defaultdict

def get_text_columns(df):
    """Return only text-like columns from DataFrame"""
    return df.select_dtypes(include=['object', 'string']).columns

def create_manticore_knn_tab():
    """Create the Manticore KNN search tab with live input and sliders"""
    return html.Div([
        # Input row containing search and sliders
        html.Div([
            # Search input
            html.Div([
                dcc.Input(
                    id='knn-search-input',
                    type='text',
                    placeholder='Enter your search query...',
                    style={
                        'width': '100%',
                        'padding': '10px',
                        'height': '40px',
                        'boxSizing': 'border-box'
                    },
                    persistence=True,
                    persistence_type='local'
                ),
            ], style={'flex': '2', 'marginRight': '20px'}),

            # EF Slider
            html.Div([
                html.Label('EF (Search Depth)', style={'marginBottom': '5px', 'display': 'block'}),
                dcc.Slider(
                    id='knn-ef-slider',
                    min=100,
                    max=5000,
                    step=100,
                    value=2000,
                    marks={i: str(i) for i in range(100, 5001, 1000)},
                    persistence=True,
                    persistence_type='local'
                ),
            ], style={'flex': '1', 'marginRight': '20px'}),

            # K Slider
            html.Div([
                html.Label('K (Results)', style={'marginBottom': '5px', 'display': 'block'}),
                dcc.Slider(
                    id='knn-k-slider',
                    min=1,
                    max=20,
                    step=1,
                    value=5,
                    marks={i: str(i) for i in range(1, 21, 2)},
                    persistence=True,
                    persistence_type='local'
                ),
            ], style={'flex': '1'}),
        ], style={
            'display': 'flex',
            'alignItems': 'center',
            'marginBottom': '10px',
            'gap': '10px',
            'width': '99%',
            'padding': '10px',
            'backgroundColor': '#f5f5f5',
            'borderRadius': '5px'
        }),

        # Query time display
        html.Div([
            html.P(id='knn-query-time', style={
                'margin': '0',
                'color': '#666',
                'fontFamily': 'monospace'
            }),
        ], style={
            'width': '99%',
            'marginBottom': '20px',
            'padding': '5px 10px',
            'backgroundColor': '#f9f9f9',
            'borderRadius': '3px'
        }),

        # Live values display
        html.Div([
            html.H3(id='knn-live-values', style={'margin': '10px 0'}),
        ], style={'width': '99%'}),

        # Results section
        html.Div(
            id='knn-search-results',
            style={
                'width': '99%',
                'padding': '10px',
                'backgroundColor': '#ffffff',
                'borderRadius': '5px',
                'border': '1px solid #ddd'
            }
        )
    ], style={
        'width': '100%',
        'padding': '20px',
        'boxSizing': 'border-box',
        'display': 'flex',
        'flexDirection': 'column',
        'alignItems': 'center'
    })

@callback(
    Output('knn-live-values', 'children'),
    Input('knn-search-input', 'value'),
    Input('knn-ef-slider', 'value'),
    Input('knn-k-slider', 'value')
)
def update_live_values(search_query, ef_value, k_value):
    if not search_query:
        return "Enter a search query to begin..."
    return f"Query: {search_query} | EF: {ef_value} | K: {k_value}"

@callback(
    [Output('knn-search-results', 'children'),
     Output('knn-query-time', 'children')],
    Input('knn-search-input', 'value'),
    Input('knn-ef-slider', 'value'),
    Input('knn-k-slider', 'value')
)
def perform_knn_search(search_query, ef_value, k_value):
    if not search_query:
        return html.Div("Enter a search query to see results", style={
            'color': '#666',
            'fontStyle': 'italic',
            'padding': '20px',
            'textAlign': 'center'
        }), ""

    try:
        # Get embedding for the search query
        t0_embed = time.time()
        model = process_3.model
        embeddings = model.encode([search_query])
        vector_values = [float(x) for x in embeddings[0]]
        vector_str = ','.join(str(x) for x in vector_values)
        t1_embed = time.time()
        embed_time_ms = (t1_embed - t0_embed) * 1000

        # Perform KNN search in Manticore
        t0_search = time.time()
        with manticore_client_weights_server() as client:
            sql = f"""
            SELECT id, table_name, table_rowid, text_str, knn_dist() as distance
            FROM text_vector_64_floats
            WHERE knn(text_vector, {k_value}, ({vector_str}), {ef_value})
            ORDER BY distance ASC
            """
            results = manticore_query(client, sql)
        t1_search = time.time()
        search_time_ms = (t1_search - t0_search) * 1000

        # We no longer need to fetch from Clickhouse as text_str is in the result
        timing_text = f"Embedding: {embed_time_ms:.1f}ms | Search: {search_time_ms:.1f}ms | Total: {(embed_time_ms + search_time_ms):.1f}ms"

        if results.empty:
            return html.Div("No results found", style={
                'color': '#666',
                'fontStyle': 'italic',
                'padding': '20px',
                'textAlign': 'center'
            }), timing_text

        # Format results
        result_items = []
        for _, row in results.iterrows():
            table_name = row['table_name']
            row_id = row['table_rowid']
            text_str = row['text_str']

            result_items.append(html.Div([
                # Header with table and distance
                html.H4(f"Table: {table_name} | Row ID: {row_id}",
                       style={'margin': '10px 0 5px 0'}),
                html.P(f"Distance: {row['distance']:.4f}",
                      style={'margin': '5px 0'}),

                # Text data from Manticore
                html.Div([
                    html.P(text_str)
                ], style={
                    'backgroundColor': '#f9f9f9',
                    'padding': '10px',
                    'borderRadius': '5px',
                    'marginTop': '5px',
                    'whiteSpace': 'pre-wrap',
                    'wordBreak': 'break-word'
                }),

                html.Hr(style={'margin': '10px 0'})
            ], style={'padding': '5px 0'}))

        return html.Div(result_items), timing_text

    except Exception as e:
        return create_error_div(e), "Error during query"