from dash import callback_context, html, dcc, callback, Output, Input, State, ALL, no_update
from py_index.database_settings import CLICKHOUSE_SETTINGS
from clickhouse_connect import get_client
from py_index.manticore_database_ops import manticore_client_data_server, manticore_query
from py_index.search_demo.components import create_data_table, create_error_div
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json
import traceback

# Custom highlight markers
HIGHLIGHTER_BEFORE_MATCH = "__before__"
HIGHLIGHTER_AFTER_MATCH = "__after__"

def create_manticore_highlights_tab():
    return html.Div([
        # Store for selected row
        dcc.Store(id='manticore-highlights-selected-row', storage_type='local'),
        # Store for popup state
        dcc.Store(id='manticore-highlights-popup-state', storage_type='local', data={'expanded': True}),
        
        # Overlay for selected row
        html.Div([
            # Header with minimize button
            html.Div([
                html.Div(id='manticore-highlights-header', style={'flex': '1'}),
                html.Button(
                    '−',  # Using minus sign as minimize icon
                    id='manticore-highlights-minimize-btn',
                    style={
                        'border': 'none',
                        'background': 'none',
                        'fontSize': '20px',
                        'cursor': 'pointer',
                        'padding': '0 5px',
                    }
                )
            ], style={
                'display': 'flex',
                'alignItems': 'center',
                'marginBottom': '10px'
            }),
            
            # Content area
            html.Div(id='manticore-highlights-content')
        ],
        id='manticore-highlights-selected-overlay',
        style={
            'position': 'fixed',
            'top': '20px',
            'right': '20px',
            'zIndex': 1000,
            'backgroundColor': 'rgba(255, 255, 255, 0.95)',
            'padding': '10px',
            'borderRadius': '5px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.2)',
            'cursor': 'pointer'
        }),
        
        # Search input box
        dcc.Input(
            id='manticore-highlights-input',
            type='text',
            placeholder='Type to see live updates...',
            style={'width': '95%', 'padding': '10px', 'marginBottom': '20px'},
            persistence=True,
            persistence_type='local',
            debounce=0.25
        ),
        
        # Live output area
        html.Div(id='manticore-highlights-output')
    ])

def highlight_text_to_spans(text):
    """Convert custom highlight markers to styled spans"""
    parts = []
    current_pos = 0
    
    while True:
        start = text.find(HIGHLIGHTER_BEFORE_MATCH, current_pos)
        if start == -1:
            # Add remaining text if any
            if current_pos < len(text):
                parts.append(html.Span(text[current_pos:], style={
                    'whiteSpace': 'pre-wrap',
                    'wordBreak': 'break-word',
                    'overflowWrap': 'break-word'
                }))
            break
            
        end = text.find(HIGHLIGHTER_AFTER_MATCH, start)
        if end == -1:
            # If no end marker, treat rest as normal text
            parts.append(html.Span(text[current_pos:], style={
                'whiteSpace': 'pre-wrap',
                'wordBreak': 'break-word',
                'overflowWrap': 'break-word'
            }))
            break
            
        # Add text before highlight if any
        if start > current_pos:
            parts.append(html.Span(text[current_pos:start], style={
                'whiteSpace': 'pre-wrap',
                'wordBreak': 'break-word',
                'overflowWrap': 'break-word'
            }))
            
        # Add highlighted text
        highlighted_text = text[start + len(HIGHLIGHTER_BEFORE_MATCH):end]
        parts.append(html.Span(
            highlighted_text,
            style={
                'backgroundColor': '#fff3b8',
                'padding': '1px 2px',
                'borderRadius': '2px',
                'fontWeight': 'bold',
                'whiteSpace': 'pre-wrap',
                'wordBreak': 'break-word',
                'overflowWrap': 'break-word'
            }
        ))
        
        current_pos = end + len(HIGHLIGHTER_AFTER_MATCH)
    
    return parts

def create_custom_data_table(df, title, table_name):
    """Custom data table that renders HTML in the highlight column"""
    return html.Div([
        html.H4(title),
        html.Table(
            # Header
            [html.Tr([
                html.Th(
                    col,
                    style={
                        'backgroundColor': '#f1f8ff',
                        'fontFamily': 'monospace',
                        'fontWeight': 'bold',
                        'padding': '10px',
                        'border': '1px solid #ddd',
                        'textAlign': 'left'
                    }
                ) for col in df.columns
            ])] +
            # Body
            [
                html.Tr([
                    html.Td(
                        # If it's a highlighted column, convert markers to spans
                        highlight_text_to_spans(str(cell).strip()[:256]) if isinstance(cell, str) and (HIGHLIGHTER_BEFORE_MATCH in str(cell))
                        else html.Div(
                            children=str(cell)[:256],
                            style={'fontFamily': 'monospace'}
                        ),
                        style={
                            'padding': '1px 1px',
                            'border': '1px solid #ddd',
                            'whiteSpace': 'pre-wrap',
                            'fontFamily': 'monospace'
                        }
                    )
                    for col, cell in row.items()
                ],
                id={'type': 'table-row', 'table': table_name, 'id': row['id']},
                style={
                    'backgroundColor': 'white',
                    'borderBottom': '1px solid #ddd',
                    'cursor': 'pointer',
                    'transition': 'background-color 0.2s'
                }
                ) for _, row in df.iterrows()
            ],
            style={
                'width': '100%',
                'borderCollapse': 'collapse',
                'border': '1px solid #ddd',
                'fontFamily': 'monospace'
            }
        )
    ])

def get_table_to_file_mapping():
    """Get a mapping of table names to their original file names"""
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        df = client.query_df('SELECT file_name, table_name FROM input_tables_summary')
        return dict(zip(df['table_name'], df['file_name']))

def format_table_display(table_name, table_to_file):
    """Format the display of a table name with its file name"""
    file_name = table_to_file.get(table_name, table_name)  # Fallback to table_name if not found
    return html.Div([
        html.Span(file_name),
        html.Span(
            f" ({table_name})",
            style={
                'color': '#666',
                'fontSize': '0.9em',
                'marginLeft': '4px'
            }
        )
    ], style={'display': 'inline'})

def create_suggestion_box(suggestions_df, title="Suggested searches across all tables:"):
    """Create a box with clickable suggestions"""
    return html.Div([
        html.H4(title, style={'marginBottom': '15px', 'color': '#666'}),
        html.Div([
            html.Button(
                [
                    html.Span(
                        suggestion,
                        style={'fontWeight': 'bold'}
                    ),
                    html.Span(
                        f" ({docs} docs)",
                        style={'color': '#666', 'fontSize': '0.9em'}
                    )
                ],
                id={'type': 'highlight-suggestion-button', 'index': i},
                style={
                    'margin': '5px',
                    'padding': '8px 15px',
                    'backgroundColor': '#f0f0f0',
                    'border': '1px solid #ddd',
                    'borderRadius': '4px',
                    'cursor': 'pointer',
                    'display': 'inline-block',
                    'fontFamily': 'monospace'
                }
            ) for i, (suggestion, docs) in enumerate(zip(suggestions_df['suggest'], suggestions_df['docs']))
        ], style={
            'display': 'flex',
            'flexWrap': 'wrap',
            'gap': '10px'
        })
    ], style={
        'backgroundColor': 'white',
        'padding': '20px',
        'borderRadius': '4px',
        'border': '1px solid #ddd',
        'marginTop': '20px'
    })

def get_suggestions_for_table(table, query):
    """Get suggestions for a single table"""
    suggest_sql = f"CALL SUGGEST('{query}', '{table}', 5 as limit)"
    with manticore_client_data_server() as client:
        suggestions_df = manticore_query(client, suggest_sql)
        if not suggestions_df.empty:
            # Convert docs to integer
            suggestions_df['docs'] = suggestions_df['docs'].astype(int)
    return suggestions_df

def aggregate_suggestions(tables, query):
    """Get and aggregate suggestions from all tables"""
    all_suggestions = []
    
    # Collect suggestions from all tables
    for table in tables:
        suggestions_df = get_suggestions_for_table(table, query)
        if not suggestions_df.empty:
            all_suggestions.append(suggestions_df)
    
    if not all_suggestions:
        return pd.DataFrame()
    
    # Combine all suggestions
    combined_df = pd.concat(all_suggestions, ignore_index=True)
    
    # Ensure docs is integer type
    combined_df['docs'] = combined_df['docs'].astype(int)
    
    # Group by suggestion and sum the docs
    aggregated_df = combined_df.groupby('suggest', as_index=False).agg({
        'docs': 'sum',  # Sum will now work correctly with integers
        'distance': 'min'  # Keep the smallest distance for sorting
    })
    
    # Sort by distance (ascending) and docs (descending)
    aggregated_df = aggregated_df.sort_values(['distance', 'docs'], ascending=[True, False])
    
    # Take top 5 suggestions
    return aggregated_df.head(5)

@callback(
    Output('manticore-highlights-output', 'children'),
    Input('manticore-highlights-input', 'value')
)
def update_output(value):
    if value is None:
        return '...'
    if value.strip() == '':
        return '...'
    if len(value) < 2:
        return '... 2 letters plz ...'
    
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        tables = client.query_df('select table_name from input_tables_summary')['table_name'].tolist()
    
    t0 = time.time()
    data = sorted(list(highlight_query_all_tables(tables, value)))
    dt_ms = (time.time() - t0) * 1000
    
    # Get table name to file name mapping
    table_to_file = get_table_to_file_mapping()
    
    # Group results by table
    table_results = {}
    for table_name, hits in data:
        if hits:  # Only add tables that have results
            df = pd.DataFrame(hits)
            df = df.sort_values('weight', ascending=False)
            table_results[table_name] = df
    
    # If no results in any table, show suggestions
    if not table_results:
        # Get aggregated suggestions from all tables
        suggestions_df = aggregate_suggestions(tables, value)
        
        if suggestions_df.empty:
            return html.H3('No matches or suggestions found', style={'color': '#666'})
        
        return [
            html.H3('No direct matches found', style={'color': '#666', 'marginBottom': '20px'}),
            create_suggestion_box(suggestions_df)
        ]
    
    # Create output elements
    output_elements = []
    
    # Add summary header
    tables_with_results = len(table_results)
    output_elements.append(
        html.H3(f'Found matches in {tables_with_results} tables (searched {len(tables)} tables in {dt_ms:.2f}ms)')
    )
    
    # Add individual table results
    for table_name, df in table_results.items():
        output_elements.extend([
            html.Hr(),  # Add separator between tables
            create_custom_data_table(
                df,
                title=format_table_display(table_name, table_to_file),
                table_name=table_name
            )
        ])
    
    return output_elements

def highlight_query_all_tables(tables, query):
    # Use ThreadPool to parallelize queries
    with ThreadPoolExecutor(max_workers=6) as executor:
        # Create a partial function with the query parameter fixed
        query_fn = partial(highlight_query_table, query=query)
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

def highlight_query_table(table, query):
    with manticore_client_data_server() as client:
        fields = manticore_query(client, f"desc {table}").to_dict(orient='records')

    fields = [field for field in fields if field['Field'] != 'id' and field['Type'] == 'text']

    other_highlights = [
            f"highlight({{allow_empty=1, before_match='{HIGHLIGHTER_BEFORE_MATCH}', after_match='{HIGHLIGHTER_AFTER_MATCH}'}}, {field['Field']}) as highlight_{field['Field']}" 
            for field in fields 
            if field['Field'] != 'id'
            and field['Type'] in ['text', 'string']
    ][:60]
    highlight_column_count = len(other_highlights)
    columns = ['id'] + other_highlights # + ['*']

    sql = f"""
    select WEIGHT() as weight, highlight({{before_match='{HIGHLIGHTER_BEFORE_MATCH}', after_match='{HIGHLIGHTER_AFTER_MATCH}'}}) as highlight_all, {", ".join(columns)} 
    from {table}
    where match(%s)
    limit 50
    """
    with manticore_client_data_server() as client:
        df = manticore_query(client, sql, (query,))
    if df.empty:
        return []
    
    # Drop columns where all values are empty strings or null
    for (i, col) in enumerate(df.columns):
        if i <= 2:
            continue
        if i > 2 + highlight_column_count:
            break
        # Check if all values in the column are either empty strings or null
        if df[col].fillna('').str.strip().eq('').all():
            df = df.drop(columns=[col])
    
    return df.to_dict('records')

@callback(
    [Output('manticore-highlights-selected-row', 'data'),
     Output({'type': 'table-row', 'table': ALL, 'id': ALL}, 'style')],
    [Input({'type': 'table-row', 'table': ALL, 'id': ALL}, 'n_clicks')],
    [State('manticore-highlights-selected-row', 'data'),
     State({'type': 'table-row', 'table': ALL, 'id': ALL}, 'id'),
     State({'type': 'table-row', 'table': ALL, 'id': ALL}, 'n_clicks_timestamp')]
)
def handle_row_click(clicks, current_selection, row_ids, timestamps):
    if not clicks or not any(clicks):
        # Initialize styles for all rows
        return None, [
            {
                'backgroundColor': 'white',
                'borderBottom': '1px solid #ddd',
                'cursor': 'pointer',
                'transition': 'background-color 0.2s'
            } for _ in row_ids
        ]
    
    # Find which row was clicked most recently using timestamps
    clicked_idx = next((i for i, (n, t) in enumerate(zip(clicks, timestamps)) 
                       if n and t == max(t for t, n in zip(timestamps, clicks) if n)), None)
    
    if clicked_idx is None:
        return None, [
            {
                'backgroundColor': 'white',
                'borderBottom': '1px solid #ddd',
                'cursor': 'pointer',
                'transition': 'background-color 0.2s'
            } for _ in row_ids
        ]
    
    clicked_row = row_ids[clicked_idx]
    
    # If clicking the same row that's selected, deselect it
    if current_selection and \
       current_selection['table'] == clicked_row['table'] and \
       current_selection['id'] == clicked_row['id']:
        new_selection = None
    else:
        new_selection = clicked_row
    
    # Update styles for all rows
    styles = []
    for row_id in row_ids:
        if new_selection and \
           row_id['table'] == new_selection['table'] and \
           row_id['id'] == new_selection['id']:
            styles.append({
                'backgroundColor': '#e3f2fd',
                'borderBottom': '1px solid #ddd',
                'cursor': 'pointer',
                'transition': 'background-color 0.2s'
            })
        else:
            styles.append({
                'backgroundColor': 'white',
                'borderBottom': '1px solid #ddd',
                'cursor': 'pointer',
                'transition': 'background-color 0.2s'
            })
    
    return new_selection, styles

def get_row_details(table_name, row_id):
    """Fetch detailed information about a specific row"""
    # with manticore_mysql_client() as client:
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        query = f"SELECT * FROM {table_name} where id = {row_id}"
        # df = manticore_query(client, query)
        df = client.query_df(query)
        if df.empty:
            return None
        return df.iloc[0].to_dict()

@callback(
    [Output('manticore-highlights-popup-state', 'data'),
     Output('manticore-highlights-minimize-btn', 'style'),
     Output('manticore-highlights-selected-overlay', 'style')],
    [Input('manticore-highlights-selected-overlay', 'n_clicks'),
     Input('manticore-highlights-minimize-btn', 'n_clicks')],
    [State('manticore-highlights-popup-state', 'data'),
     State('manticore-highlights-selected-row', 'data')],
    prevent_initial_call=True
)
def toggle_popup_state(overlay_clicks, minimize_clicks, current_state, selected_row):
    if not selected_row:
        return no_update, no_update, no_update
        
    if not overlay_clicks and not minimize_clicks:
        return no_update, no_update, no_update
        
    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update, no_update
        
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    base_minimize_style = {
        'border': 'none',
        'background': 'none',
        'fontSize': '20px',
        'cursor': 'pointer',
        'padding': '0 5px'
    }
    
    base_overlay_style = {
        'position': 'fixed',
        'top': '20px',
        'right': '20px',
        'zIndex': 1000,
        'backgroundColor': 'rgba(255, 255, 255, 0.95)',
        'padding': '10px',
        'borderRadius': '5px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.2)',
        'cursor': 'pointer'
    }
    
    if trigger_id == 'manticore-highlights-selected-overlay' and not current_state['expanded']:
        # Expanding
        return (
            {'expanded': True},
            {**base_minimize_style, 'display': 'block'},
            {**base_overlay_style, 'width': '60vw', 'maxWidth': '60vw', 'maxHeight': '80vh', 'wordWrap': 'break-word'}
        )
    elif trigger_id == 'manticore-highlights-minimize-btn':
        # Minimizing
        return (
            {'expanded': False},
            {**base_minimize_style, 'display': 'none'},
            base_overlay_style
        )
    
    return no_update, no_update, no_update

def get_column_name_mapping(table_name):
    """Get a mapping of fixed column names to their original names"""
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        print(f"Fetching column mapping for table: {table_name}")
        df = client.query_df(f"""
            SELECT column_name, column_name_fixed
            FROM input_tables_raw_columns c inner join input_tables_recreated r 
                on c.table_name = r.original_table_name
            WHERE r.table_name = '{table_name}'

            limit 1000
           """)
        print(f"Found {len(df)} rows")
        if not df.empty:
            print("Sample mappings:")
            print(df.head())
        if df.empty:
            print("No mappings found!")
            return {}
        mapping = dict(zip(df['column_name_fixed'], df['column_name']))
        print(f"Created mapping with {len(mapping)} entries")
        return mapping

@callback(
    [Output('manticore-highlights-header', 'children'),
     Output('manticore-highlights-content', 'children')],
    [Input('manticore-highlights-selected-row', 'data'),
     Input('manticore-highlights-popup-state', 'data'),
     Input('manticore-highlights-input', 'value')]
)
def update_selected_overlay(selected_row, popup_state, search_query):
    if not selected_row:
        return html.P('No Selection', style={'fontStyle': 'italic'}), ''
    
    table_name = selected_row['table']
    table_to_file = get_table_to_file_mapping()
    row_id = selected_row['id']
    
    if not popup_state['expanded']:
        return (
            html.H3([
                'Selected: ',
                format_table_display(table_name, table_to_file),
                html.Span(f' - ROW# {row_id}')
            ], style={'margin': '0', 'cursor': 'pointer'}),
            ''
        )
    
    # Fetch detailed row information
    row_details = get_row_details(table_name, row_id)
    
    # Get column name mapping
    column_name_mapping = get_column_name_mapping(table_name)
    
    def highlight_text(text, query):
        if not query:
            return text
            
        # Split query into words and convert to lowercase for case-insensitive matching
        query_words = [word.lower() for word in query.split()]
        text_lower = text.lower()
        
        # Find all positions where query words match
        matches = []
        for word in query_words:
            start = 0
            while True:
                pos = text_lower.find(word, start)
                if pos == -1:
                    break
                matches.append((pos, pos + len(word)))
                start = pos + 1
        
        if not matches:
            return text
            
        # Sort matches and merge overlapping spans
        matches.sort()
        merged = []
        current_start, current_end = matches[0]
        
        for start, end in matches[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))
        
        # Build highlighted text with spans
        result = []
        last_end = 0
        for start, end in merged:
            if start > last_end:
                result.append(text[last_end:start])
            result.append(HIGHLIGHTER_BEFORE_MATCH)
            result.append(text[start:end])
            result.append(HIGHLIGHTER_AFTER_MATCH)
            last_end = end
        if last_end < len(text):
            result.append(text[last_end:])
        
        return ''.join(result)
    
    detail_lines = []
    if row_details:
        # Sort columns alphabetically, but skip 'id' as we don't display it
        sorted_columns = sorted(key for key in row_details.keys() if key != 'id')
        
        # Create table header
        detail_lines.append(html.Table([
            # Table header
            html.Thead(
                html.Tr([
                    html.Th('Column', style={
                        'textAlign': 'left',
                        'padding': '8px',
                        'backgroundColor': '#f5f5f5',
                        'borderBottom': '2px solid #ddd',
                        'fontFamily': 'monospace',
                        'whiteSpace': 'nowrap',
                        'width': '11rem',
                        'minWidth': '11rem'
                    }),
                    html.Th('Content', style={
                        'textAlign': 'left',
                        'padding': '8px',
                        'backgroundColor': '#f5f5f5',
                        'borderBottom': '2px solid #ddd',
                        'fontFamily': 'monospace'
                    })
                ])
            ),
            # Table body
            html.Tbody([
                html.Tr([
                    # Column name cell with both original and fixed names
                    html.Td([
                        # Original column name in h4
                        html.H4(
                            column_name_mapping.get(key, key),  # Original name from mapping
                            style={
                                'margin': '0 0 4px 0',
                                'fontSize': '1rem',
                                'fontWeight': 'bold'
                            }
                        ),
                        # Fixed column name in gray
                        html.Div(
                            key,  # Fixed name (c000_something)
                            style={
                                'color': '#666',
                                'fontSize': '0.8rem'
                            }
                        )
                    ], style={
                        'padding': '8px',
                        'borderBottom': '1px solid #eee',
                        'fontFamily': 'monospace',
                        'verticalAlign': 'top',
                        'whiteSpace': 'nowrap',
                        'width': '11rem',
                        'minWidth': '11rem'
                    }),
                    # Content cell with highlighting
                    html.Td(
                        html.Div(
                            highlight_text_to_spans(highlight_text(str(row_details[key]), search_query)),
                            style={
                                'fontFamily': 'monospace',
                                'whiteSpace': 'pre-wrap',
                                'wordBreak': 'break-word',
                                'overflowWrap': 'break-word',
                                'padding': '8px 0'
                            }
                        ),
                        style={
                            'padding': '8px',
                            'borderBottom': '1px solid #eee'
                        }
                    )
                ]) for key in sorted_columns if str(row_details[key]).strip()  # Only show non-empty values
            ])
        ], style={
            'borderCollapse': 'collapse',
            'marginTop': '10px',
            'width': '100%',
            'maxWidth': '60vw'
        }))
    
    header = html.H3([
        'Details for ',
        format_table_display(table_name, table_to_file)
    ], style={'margin': '0', 'flex': '1'})
    
    content = html.Div([
        # Raw values section
        html.Div([
            html.Strong('File: '), format_table_display(table_name, table_to_file),
            html.Br(),
            html.Strong('ROW# '), html.Code(str(row_id)),
            html.Hr(style={'margin': '10px 0'})
        ]),
        
        # Query results section
        html.Div(
            detail_lines if detail_lines else "No additional details found",
            style={
                'maxHeight': 'calc(80vh - 150px)',  # Account for header and raw values
                'overflowY': 'auto',
                'padding': '5px',
                'maxWidth': '60vw'
            }
        )
    ])
    
    return header, content

@callback(
    [Output('manticore-highlights-input', 'value'),
     Output('manticore-highlights-input', 'n_submit')],
    [Input({'type': 'highlight-suggestion-button', 'index': ALL}, 'n_clicks')],
    [State({'type': 'highlight-suggestion-button', 'index': ALL}, 'children')]
)
def handle_suggestion_click(clicks, children):
    if not any(clicks) or not clicks:
        return no_update, no_update
    
    # Find which button was clicked
    ctx = callback_context
    if not ctx.triggered:
        return no_update, no_update
    
    button_id = ctx.triggered[0]['prop_id']
    try:
        clicked_index = json.loads(button_id.split('.')[0])['index']
    except:
        return no_update, no_update
    
    # Get the suggestion text (first span child contains the suggestion)
    suggestion = children[clicked_index][0]['props']['children']
    
    # Return the suggestion and trigger a new submit
    return suggestion, 1