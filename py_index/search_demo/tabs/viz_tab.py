import clickhouse_connect
from dash import html, dcc, callback, Input, Output
from py_index.database_settings import CLICKHOUSE_SETTINGS
from py_index.search_demo.components import create_error_div

# List of visualization URLs - we'll start with one and expand later
SUPERSET_IDS = [
    "oGKAlvRlWmw", "1yY4laelB30",
]

def get_table_options():
    """Get table options from Clickhouse, only for tables that have charts"""
    with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as client:
        df = client.query_df('''
            SELECT DISTINCT i.file_name, i.table_name
            FROM input_tables_summary i
            INNER JOIN superset_charts c ON c.table_name = i.table_name
        ''')
        if df.empty:
            return []
        # Sort by table_name
        df = df.sort_values('table_name')
        # Create options list with table_name - file_name format
        options = [{'label': f'{table_name} - "{file_name}"', 'value': table_name}
                  for file_name, table_name in zip(df['file_name'], df['table_name'])]
        return options

def superset_embed_url(superset_id):
    # http://localhost:8088/explore/?slice_id=380
    return f"http://localhost:28088/superset/explore/?slice_id={superset_id}&standalone=1&height=400"


def superset_open_url(superset_id):
    return f"http://localhost:28088/superset/explore/?slice_id={superset_id}"

def create_viz_card(superset_id, card_title):
    """Create a card containing an iframe visualization"""
    return         html.Div([
            html.A(
                html.H3(
                    card_title,
                ),

                href=superset_open_url(superset_id),
                target="_blank",
                style={
                    'position': 'absolute',
                    'top': '0',
                    'left': '0',
                    'textDecoration': 'none',
                    'color': 'inherit',
                    "padding": "1px",
                    "margin": "1px",
                    "display": "inline-block",
                    "background": "rgba(255, 255, 255, 0.1)",
                    "zIndex": 4,
                }
            ),
            html.Iframe(
                src=superset_embed_url(superset_id),
                width="600",
                height="400",
                style={
                    'position': 'absolute',
                    'top': '0',
                    'left': '0',
                    'border': 'none',
                    'scrolling': 'no',
                    "borderRadius": "8px",
                    "padding": "10px",
                    "margin": "10px",
                    "width": "600px",
                    "height": "400px",
                    "zIndex": 2,
                }
            ),
        ], style={
            "position": "relative",
            "width": "622px",
            "height": "422px",
            "display": "inline-block",
            "background": "rgba(255, 255, 255, 0.5)",
        })

def create_viz_grid(charts):
    """Create a grid of visualization cards"""
    if not charts:
        return html.Div("No charts available for this table", style={
            'color': '#666',
            'fontStyle': 'italic',
            'padding': '20px',
            'textAlign': 'center'
        })

    return html.Div([
        create_viz_card(chart['superset_chart_id'], chart['superset_chart_name']) for chart in charts
    ], style={
        'display': 'grid',
        'gridTemplateColumns': 'repeat(auto-fit, minmax(620px, 1fr))',
        'gap': '2px',
        'padding': '2px',
        'backgroundColor': '#f5f5f5',
        'minHeight': '80vh',
        'overflowY': 'auto',
    })

def create_viz_tab():
    """Create the visualization tab with a table selector and grid of cards"""
    return html.Div([
        # Table selector dropdown
        html.Div([
            dcc.Dropdown(
                id='viz-table-selector',
                options=get_table_options(),
                placeholder='Select a table to view its visualizations...',
                persistence=True,
                persistence_type='local',
                style={'width': '100%'}
            ),
        ], style={'marginBottom': '20px'}),

        # Grid container for visualization cards
        html.Div(id='viz-grid-container')
    ])

@callback(
    Output('viz-grid-container', 'children'),
    Input('viz-table-selector', 'value')
)
def update_viz_grid(selected_table):
    try:
        if not selected_table:
            return html.Div("Please select a table to view its visualizations", style={
                'color': '#666',
                'fontStyle': 'italic',
                'padding': '20px',
                'textAlign': 'center'
            })

        with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as c:
            charts = c.query_df(
                "SELECT * FROM superset_charts WHERE table_name = %s",
                parameters=(selected_table,)
            ).to_dict(orient='records')

        return create_viz_grid(charts)
    except Exception as e:
        return create_error_div(e)