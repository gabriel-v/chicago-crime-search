from dash import Dash, html, dcc
from py_index.search_demo.tabs.clickhouse_tab import create_clickhouse_tab
from py_index.search_demo.tabs.manticore_tab import create_manticore_tab
from py_index.search_demo.tabs.manticore_autocomplete_tab import create_manticore_autocomplete_tab
from py_index.search_demo.tabs.manticore_highlights_tab import create_manticore_highlights_tab
from py_index.search_demo.tabs.manticore_facet_tab import create_manticore_facet_tab
from py_index.search_demo.tabs.viz_tab import create_viz_tab
from py_index.search_demo.tabs.manticore_knn_tab import create_manticore_knn_tab

# Initialize the app
app = Dash(__name__)

app.layout = html.Div([
    # Store component for persisting tab selection
    dcc.Store(id='selected-tab', storage_type='local'),
    
    html.H1("Search Demo", style={'textAlign': 'center'}),
    
    dcc.Tabs([
        dcc.Tab(label='Clickhouse Raw Query', value='tab-clickhouse', children=[
            create_clickhouse_tab()
        ]),
        dcc.Tab(label='Manticore Raw Query', value='tab-manticore', children=[
            create_manticore_tab()
        ]),
        dcc.Tab(label='Manticore Autocomplete', value='tab-autocomplete', children=[
            create_manticore_autocomplete_tab()
        ]),
        dcc.Tab(label='Manticore Highlights', value='tab-highlights', children=[
            create_manticore_highlights_tab()
        ]),
        dcc.Tab(label='Manticore Facet Search', value='tab-facet', children=[
            create_manticore_facet_tab()
        ]),
        dcc.Tab(label='Manticore KNN Search', value='tab-knn', children=[
            create_manticore_knn_tab()
        ]),
        dcc.Tab(label='Visualizations', value='tab-viz', children=[
            create_viz_tab()
        ]),
    ], id='tabs', persistence=True, persistence_type='local')
])

if __name__ == "__main__":
    app.run(debug=True, host='localhost', port=8099)