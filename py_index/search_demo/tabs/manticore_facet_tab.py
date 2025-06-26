from dash import html, dcc, callback, Output, Input, State, ALL, callback_context, no_update, dash_table
from py_index.database_settings import CLICKHOUSE_SETTINGS
from clickhouse_connect import get_client
from py_index.manticore_database_ops import manticore_client_data_server, manticore_query
from py_index.search_demo.components import create_data_table, create_sql_query_display, create_facet_table, create_highlighted_data_table, highlight_text_to_spans, create_error_div
import pandas as pd
import json
import datetime
import time
import traceback

# Custom highlight markers
HIGHLIGHTER_BEFORE_MATCH = "__before__"
HIGHLIGHTER_AFTER_MATCH = "__after__"

def get_table_options():
    """Get table options from Clickhouse"""
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        df = client.query_df('SELECT file_name, table_name FROM input_tables_summary')
        # Sort by table_name
        df = df.sort_values('table_name')
        # Create options list with table_name - file_name format
        options = [{'label': f'{table_name} - "{file_name}"', 'value': table_name} 
                  for file_name, table_name in zip(df['file_name'], df['table_name'])]
        return options

def create_manticore_facet_tab():
    return html.Div([
        # Table selector dropdown
        html.Div([
            dcc.Dropdown(
                id='manticore-facet-table-selector',
                options=get_table_options(),
                placeholder='Select a table...',
                persistence=True,
                persistence_type='local',
                style={'width': '100%', 'marginBottom': '20px'}
            ),
        ]),
        
        # Search input and count display container
        html.Div([
            # Search input box (only shown when table is selected)
            html.Div([
                dcc.Input(
                    id='manticore-facet-search-input',
                    type='text',
                    placeholder='Type to see live updates...',
                    style={'width': '100%', 'padding': '10px'},
                    persistence=True,
                    persistence_type='local',
                    debounce=0.25
                ),
            ], style={'width': '33%', 'display': 'inline-block'}),
            
            # Empty middle space
            html.Div(style={'width': '33%', 'display': 'inline-block'}),
            
            # Total matches display
            html.Div([
                html.H4(
                    id='manticore-facet-total-matches',
                    style={'margin': '0', 'lineHeight': '38px'}  # Match input height
                )
            ], style={'width': '33%', 'display': 'inline-block', 'textAlign': 'right'})
        ], id='manticore-facet-search-container', style={'display': 'none', 'marginBottom': '20px'}),
        
        # Facets section with loading spinner
        # dcc.Loading(
        #     id="facets-loading",
        #     type="circle",
        #     children=html.Div(id='manticore-facet-facets'),
        #     style={'marginBottom': '20px'}
        # ),
        html.Div(id='manticore-facet-facets'),
        
        # Results section with loading spinner
        dcc.Loading(
            id="results-loading",
            type="circle",
            children=html.Div(id='manticore-facet-results')
        ),
        
        # Store for SQL query
        dcc.Store(id='manticore-facet-sql-query'),
        
        # Store for suggestion clicks
        dcc.Store(id='manticore-facet-suggestion-click', data=''),

        # Store for facet filter states
        dcc.Store(id='manticore-facet-filter-states', data={}),
    ])

def create_custom_data_table(df, title):
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
                style={
                    'backgroundColor': 'white',
                    'borderBottom': '1px solid #ddd',
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

def build_filter_conditions(filter_states):
    """Build SQL filter conditions from filter states"""
    conditions = []
    
    if not filter_states:
        return conditions
        
    for field, state in filter_states.items():
        field_type = state['type']
        values = state['values']
        
        if not values:
            continue
            
        if field_type == 'string':
            # For string fields, use IN clause
            values_str = ', '.join(f"'{v}'" for v in values)
            conditions.append(f"{field} IN ({values_str})")
        elif field_type in ['bigint', 'timestamp', 'float', 'double']:
            # For numeric fields, parse range strings
            range_conditions = []
            for value in values:
                if ' to ' in value:
                    start, end = value.split(' to ')
                    # For timestamps, try to parse datetime string, if fails assume it's already a timestamp
                    if field_type == 'timestamp':
                        try:
                            start = int(datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S').timestamp())
                        except ValueError:
                            # If parsing fails, assume it's already a numeric timestamp
                            start = int(float(start))
                        try:
                            end = int(datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S').timestamp())
                        except ValueError:
                            # If parsing fails, assume it's already a numeric timestamp
                            end = int(float(end))
                    else:
                        # Remove any non-numeric characters
                        start = ''.join(c for c in start if c.isdigit() or c == '.' or c == '-')
                        end = ''.join(c for c in end if c.isdigit() or c == '.' or c == '-')
                        
                        # Convert to appropriate type
                        if field_type in ['float', 'double']:
                            start = float(start)
                            end = float(end)
                        else:
                            start = int(start)
                            end = int(end)
                            
                        # Handle edge case where start and end are the same
                        if start == end:
                            # For integers, use exact match
                            if field_type == 'bigint':
                                range_conditions.append(f"{field} = {start}")
                                continue
                            # For floats, use small epsilon range
                            else:
                                epsilon = 0.00001
                                end = end + epsilon
                    
                    # Check if this is the last interval by comparing with the max value
                    is_last_interval = False
                    if field_type == 'timestamp':
                        # For timestamps, convert end to datetime for comparison
                        try:
                            end_dt = datetime.datetime.fromtimestamp(end)
                            # You might want to adjust this based on your data
                            max_dt = datetime.datetime(2025, 1, 1)  # Some future date
                            is_last_interval = end_dt >= max_dt
                        except:
                            is_last_interval = False
                    else:
                        # For numeric fields, compare with a large number
                        is_last_interval = end >= 1e9  # Arbitrary large number
                    
                    # Use >= for start and < for end (except for last interval where we use <=)
                    if is_last_interval:
                        range_conditions.append(f"({field} >= {start} AND {field} <= {end})")
                    else:
                        range_conditions.append(f"({field} >= {start} AND {field} < {end})")
                        
            if range_conditions:
                conditions.append(f"({' OR '.join(range_conditions)})")
    
    return conditions

def translate_interval_value(interval_idx, min_val, max_val):
    """Translate interval index (0-9) back to actual range
    Uses [a,b) intervals (inclusive start, exclusive end) for all intervals except the last one
    For the last interval, uses [a,b] (inclusive on both ends) to include the maximum value
    For integers, adjusts the max value to ensure even distribution
    """
    # For integers, adjust max_val to ensure even distribution
    if isinstance(min_val, int) and isinstance(max_val, int):
        # Add 1 to max_val to ensure the last value is included in a proper interval
        max_val = max_val + 1
    
    step = (max_val - min_val) / 9  # Use 9 steps for 10 intervals
    start = min_val + (step * interval_idx) if interval_idx > 0 else min_val
    end = min_val + (step * (interval_idx + 1))
    
    # For integers, ensure we maintain integer boundaries
    if isinstance(min_val, int) and isinstance(max_val, int):
        start = int(start)
        end = int(end)
        # For the last interval, subtract 1 from max_val to get back to the actual maximum value
        if interval_idx == 9:
            end = max_val - 1
    
    # Handle edge case where start and end are the same
    if abs(end - start) < 0.00001:  # For floats, use small epsilon
        if interval_idx == 9:  # Last interval
            # For last interval, extend slightly to include the max value
            end = max_val
        else:
            # For other intervals, extend slightly to next step boundary
            end = start + step
    
    return start, end

def prepare_search_query(table_name, filter_states=None, include_filters=True, include_main_results=True, exclude_field_from_filters=None, search_query=None):
    """Prepare the SQL query for searching
    
    Args:
        table_name: Name of the table to query
        filter_states: Dictionary of filter states
        include_filters: Whether to include filter conditions in WHERE clause
        include_main_results: Whether to include main results (False for facet-only queries)
        exclude_field_from_filters: Field to exclude from filter conditions (for facet counts)
        search_query: The search query string. If None or empty, will use '1=1' instead of match(%s)
    """
    # Get field information to identify string fields for faceting
    fields_df = get_table_structure(table_name)
    string_fields = fields_df[fields_df['Type'] == 'string']['Field'].tolist()
    numeric_fields = fields_df[fields_df['Type'].isin(['bigint', 'timestamp', 'float', 'double'])]['Field'].tolist()
    
    # Get numeric field stats for intervals
    numeric_stats = get_numeric_field_stats(table_name, fields_df)
    
    # Base query
    if include_main_results:
        columns = ['id', '*']
        if search_query and len(search_query.strip()) > 0:
            sql = f"""SELECT WEIGHT() weight, highlight({{before_match='{HIGHLIGHTER_BEFORE_MATCH}', after_match='{HIGHLIGHTER_AFTER_MATCH}'}}) highlight_all, {", ".join(columns)} FROM {table_name} WHERE match(%s)"""
        else:
            sql = f"""SELECT WEIGHT() weight, {", ".join(columns)} FROM {table_name} WHERE 1=1"""
    else:
        if search_query and len(search_query.strip()) > 0:
            sql = f"""SELECT id FROM {table_name} WHERE match(%s)"""
        else:
            sql = f"""SELECT id FROM {table_name} WHERE 1=1"""
    
    # Add filter conditions if requested
    if include_filters and filter_states:
        conditions = []
        for field, state in filter_states.items():
            # Skip this field's conditions if it's the one we're calculating facets for
            if field == exclude_field_from_filters:
                continue
                
            field_type = state['type']
            values = state['values']
            
            if not values:
                continue
                
            if field_type == 'string':
                # For string fields, use IN clause
                values_str = ', '.join(f"'{v}'" for v in values)
                conditions.append(f"{field} IN ({values_str})")
            elif field_type in ['bigint', 'timestamp', 'float', 'double']:
                # For numeric fields, parse range strings
                range_conditions = []
                for value in values:
                    if ' to ' in value:
                        start, end = value.split(' to ')
                        # For timestamps, convert datetime string back to unix timestamp
                        if field_type == 'timestamp':
                            start = int(datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S').timestamp())
                            end = int(datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S').timestamp())
                        else:
                            # Remove any non-numeric characters
                            start = ''.join(c for c in start if c.isdigit() or c == '.' or c == '-')
                            end = ''.join(c for c in end if c.isdigit() or c == '.' or c == '-')
                            
                            # Convert to appropriate type
                            if field_type in ['float', 'double']:
                                start = float(start)
                                end = float(end)
                            else:
                                start = int(start)
                                end = int(end)
                            
                            # Handle edge case where start and end are the same
                            if start == end:
                                # For integers, use exact match
                                if field_type == 'bigint':
                                    range_conditions.append(f"{field} = {start}")
                                    continue
                                # For floats, use small epsilon range
                                else:
                                    epsilon = 0.00001
                                    end = end + epsilon
                        
                        # Use inclusive range
                        range_conditions.append(f"({field} >= {start} AND {field} <= {end})")
                if range_conditions:
                    conditions.append(f"({' OR '.join(range_conditions)})")
        
        if conditions:
            sql += "\nAND " + "\nAND ".join(conditions)
    
    # Add limit only if including main results
    if include_main_results:
        sql += "\nLIMIT 20"
    else:
        sql += "\nLIMIT 0"  # No main results needed for facet-only query
    
    # Add facet clauses for string fields
    for field in string_fields:
        sql += f"\nFACET {field} ORDER BY COUNT(*) DESC LIMIT 10"
    
    # Add interval facets for numeric fields
    for field in numeric_fields:
        stats = numeric_stats.get(field)
        if stats and stats['min'] is not None and stats['max'] is not None:
            min_val = float(stats['min'])
            max_val = float(stats['max'])
            if min_val < max_val:  # Only create intervals if we have a valid range
                # For integers, adjust max value to ensure even distribution
                if fields_df[fields_df['Field'] == field].iloc[0]['Type'] in ['bigint']:
                    max_val = float(int(max_val) + 1)
                
                # Generate 9 points to create 10 intervals
                step = (max_val - min_val) / 9
                # For timestamp and bigint, ensure we use integers
                if fields_df[fields_df['Field'] == field].iloc[0]['Type'] in ['timestamp', 'bigint']:
                    interval_points = [int(min_val + step * i) for i in range(1, 10)]
                    interval_str = ','.join(str(point) for point in interval_points)
                else:
                    # For float/double, keep decimal precision but limit to 2 decimal places
                    interval_points = [round(min_val + step * i, 2) for i in range(1, 10)]
                    interval_str = ','.join(str(point) for point in interval_points)
                sql += f"\nFACET INTERVAL({field}, {interval_str}) {field}_range ORDER BY COUNT(*) DESC"
    
    return sql

@callback(
    Output('manticore-facet-search-container', 'style'),
    Input('manticore-facet-table-selector', 'value')
)
def toggle_search_input(selected_table):
    if not selected_table:
        return {'display': 'none'}
    return {'display': 'block', 'marginBottom': '20px'}

def get_table_structure(table_name):
    """Get the table structure from Manticore"""
    with manticore_client_data_server() as client:
        return manticore_query(client, f"DESC {table_name}")

def get_numeric_field_stats(table_name, fields_df):
    """Get min/max values for numeric fields"""
    # Filter for numeric fields
    numeric_fields = fields_df[
        fields_df['Type'].isin(['bigint', 'timestamp', 'float', 'double'])
    ]['Field'].tolist()
    
    if not numeric_fields:
        return {}
    
    # Build aggregate query
    agg_parts = []
    for field in numeric_fields:
        agg_parts.extend([
            f"min({field}) as min_{field}",
            f"max({field}) as max_{field}"
        ])
    
    query = f"SELECT {', '.join(agg_parts)} FROM {table_name}"
    
    # Execute query
    with manticore_client_data_server() as client:
        stats_df = manticore_query(client, query)
        if stats_df.empty:
            return {}
            
        # Convert the single row to a dict of field stats
        stats = {}
        for field in numeric_fields:
            min_val = stats_df[f'min_{field}'].iloc[0]
            max_val = stats_df[f'max_{field}'].iloc[0]
            if min_val is not None and max_val is not None:  # Only store if we got valid values
                stats[field] = {'min': min_val, 'max': max_val}
        return stats

def format_value(value, field_type):
    """Format values based on their type"""
    if field_type == 'timestamp':
        try:
            return datetime.datetime.fromtimestamp(int(value)).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return str(value)
    elif field_type in ['float', 'double']:
        try:
            return f"{float(value):.2f}"
        except:
            return str(value)
    return str(value)

def create_facet_table(data, field_name, field_type, filter_states=None):
    """Create a custom HTML table for facet data with checkboxes for WHERE clause filtering"""
    if data is None or data.empty:
        return html.Div("No data", style={'color': '#666', 'fontStyle': 'italic'})

    # Get currently selected values for this field
    selected_values = []
    if filter_states and field_name in filter_states:
        selected_values = filter_states[field_name]['values']

    # Helper function to create a row
    def create_row(value, count, is_selected, is_missing=False):
        style = {
            'backgroundColor': '#f5f5f5' if is_missing else 'white',
            'transition': 'background-color 0.2s',
            ':hover': {
                'backgroundColor': '#f0f0f0' if is_missing else '#f5f5f5'
            }
        }
        if is_selected:
            style['borderLeft'] = '3px solid #4CAF50'  # Green highlight for selected items
        
        # Format count as integer
        count_display = '0' if is_missing else str(int(float(count)))
        
        return html.Tr([
            # Value cell
            html.Td(
                str(value),
                style={
                    'padding': '6px',
                    'borderBottom': '1px solid #eee',
                    'fontFamily': 'monospace',
                    'fontSize': '14px',
                    'maxWidth': '280px',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'whiteSpace': 'nowrap'
                }
            ),
            # Count cell
            html.Td(
                count_display,
                style={
                    'padding': '6px',
                    'borderBottom': '1px solid #eee',
                    'fontFamily': 'monospace',
                    'fontSize': '14px',
                    'textAlign': 'right',
                    'width': '60px',
                    'color': '#999' if is_missing else 'inherit'
                }
            ),
            # Checkbox cell
            html.Td(
                dcc.Checklist(
                    id={
                        'type': 'facet-filter',
                        'field': field_name,
                        'value': str(value),
                        'field_type': field_type
                    },
                    options=[{'label': '', 'value': 'on'}],
                    value=['on'] if is_selected else [],
                    style={
                        'margin': '0',
                        'padding': '0',
                        'display': 'flex',
                        'justifyContent': 'center'
                    }
                ),
                style={
                    'padding': '6px',
                    'borderBottom': '1px solid #eee',
                    'textAlign': 'center',
                    'width': '40px'
                }
            )
        ], style=style)

    # First, add rows for selected values that aren't in the current data
    selected_rows = []
    unselected_rows = []
    seen_values = set()  # Track values we've seen in the data

    # Process the actual facet data
    for idx, row in data.iterrows():
        try:
            # Get the value and count from the row
            if field_type in ['bigint', 'timestamp', 'float', 'double']:
                # For numeric ranges, get the range index and count
                range_col = f'{field_name}_range'
                if range_col not in row:
                    # Skip this row if the range column is missing
                    continue
                interval_idx = row[range_col]
                count = row['count(*)']  # Count column is always named 'count(*)'
                # Convert range index to actual range string
                if 'min' in row and 'max' in row and row['min'] is not None and row['max'] is not None:
                    min_val = float(row['min'])
                    max_val = float(row['max'])
                    if min_val >= max_val:
                        # Skip if min >= max as no valid intervals can be created
                        continue
                    
                    # For integers, adjust max value to ensure even distribution
                    if field_type == 'bigint':
                        max_val = float(int(max_val) + 1)
                    
                    start, end = translate_interval_value(interval_idx, min_val, max_val)
                    
                    if field_type == 'timestamp':
                        start_str = datetime.datetime.fromtimestamp(int(start)).strftime('%Y-%m-%d %H:%M:%S')
                        end_str = datetime.datetime.fromtimestamp(int(end)).strftime('%Y-%m-%d %H:%M:%S')
                        value = f"{start_str} to {end_str}"
                    else:
                        # Format numbers based on field type
                        if field_type in ['float', 'double']:
                            # For floats, keep 2 decimal places
                            start_str = f"{float(start):.2f}"
                            end_str = f"{float(end):.2f}"
                            
                            # Handle edge case where start and end are very close
                            if abs(float(end) - float(start)) < 0.00001:
                                value = f"{start_str}"  # Just show the single value
                            else:
                                value = f"{start_str} to {end_str}"
                        else:
                            # For integers (bigint)
                            start_int = int(start)
                            end_int = int(end)
                            
                            # Handle edge case where start and end are the same
                            if start_int == end_int:
                                value = str(start_int)  # Just show the single value
                            else:
                                value = f"{start_int} to {end_int}"
                else:
                    # Skip if we don't have valid min/max values
                    continue
            else:
                # For string fields, first column is the value, second is count
                if field_name not in row or 'count(*)' not in row:
                    # Skip if required columns are missing
                    continue
                value = row[field_name]
                count = row['count(*)']

            seen_values.add(str(value))
            is_selected = str(value) in selected_values
            
            if is_selected:
                selected_rows.append(create_row(value, count, True))
            else:
                unselected_rows.append(create_row(value, count, False))
        except Exception as e:
            # Log the error and continue with next row
            print(f"Error processing row for field {field_name}: {e}")
            continue

    # Add any selected values that weren't in the facet results
    for value in selected_values:
        if str(value) not in seen_values:
            selected_rows.append(create_row(value, 0, True, True))

    # Combine rows with selected items at the top
    rows = selected_rows + unselected_rows

    # If no data and no selected values, show "No data" message
    if not rows:
        return html.Div("No data", style={'color': '#666', 'fontStyle': 'italic'})

    return html.Div([
        html.Table(
            # Header
            [html.Thead(html.Tr([
                html.Th(
                    "Value",
                    style={
                        'padding': '8px 6px',
                        'backgroundColor': '#f5f5f5',
                        'borderBottom': '2px solid #ddd',
                        'textAlign': 'left',
                        'fontSize': '13px',
                        'fontWeight': 'bold',
                        'color': '#666'
                    }
                ),
                html.Th(
                    "Count",
                    style={
                        'padding': '8px 6px',
                        'backgroundColor': '#f5f5f5',
                        'borderBottom': '2px solid #ddd',
                        'textAlign': 'right',
                        'fontSize': '13px',
                        'fontWeight': 'bold',
                        'color': '#666',
                        'width': '60px'
                    }
                ),
                html.Th(
                    "Filter",
                    style={
                        'padding': '8px 6px',
                        'backgroundColor': '#f5f5f5',
                        'borderBottom': '2px solid #ddd',
                        'textAlign': 'center',
                        'fontSize': '13px',
                        'fontWeight': 'bold',
                        'color': '#666',
                        'width': '40px'
                    }
                )
            ]))] +
            # Body
            rows,
            style={
                'width': '100%',
                'borderCollapse': 'collapse',
                'backgroundColor': 'white'
            }
        )
    ], style={
        'maxHeight': '272px',
        'overflowY': 'auto',
        'border': '1px solid #ddd',
        'borderRadius': '4px'
    })

def create_facet_box(field_info, facet_data=None, numeric_stats=None, filter_states=None):
    """Create a box displaying field information in a more compact and readable format"""
    field_name = field_info['Field']
    field_type = field_info['Type']
    
    children = [
        # Field name as header
        html.H4(field_name, style={
            'margin': '0 0 8px 0',
            'fontSize': '16px',
            'fontWeight': 'bold',
            'color': '#333'
        }),
        # Type info
        html.Div([
            html.Span("Type: ", style={'color': '#666'}),
            html.Span(field_type, style={'fontFamily': 'monospace', 'fontSize': '14px'})
        ], style={'marginBottom': '5px'}),
        # Properties (if any)
        html.Div([
            html.Span("Properties: ", style={'color': '#666'}),
            html.Span(
                "indexed stored" if "Properties" in field_info else "none",
                style={'fontFamily': 'monospace', 'color': '#0066cc', 'fontSize': '14px'}
            )
        ])
    ]
    
    # Add numeric stats if available
    if numeric_stats:
        children.extend([
            html.Hr(style={'margin': '10px 0'}),
            html.Div([
                html.Div([
                    html.Span("Min: ", style={'color': '#666'}),
                    html.Span(
                        format_value(numeric_stats['min'], field_type),
                        style={'fontFamily': 'monospace', 'fontSize': '14px'}
                    )
                ], style={'marginBottom': '3px'}),
                html.Div([
                    html.Span("Max: ", style={'color': '#666'}),
                    html.Span(
                        format_value(numeric_stats['max'], field_type),
                        style={'fontFamily': 'monospace', 'fontSize': '14px'}
                    )
                ])
            ])
        ])
    
    # Add facet data for string fields or numeric histogram
    if facet_data is not None and not facet_data.empty:
        children.extend([
            html.Hr(style={'margin': '10px 0'}),
            create_facet_table(facet_data, field_name, field_type, filter_states)
        ])
    
    return html.Div(children, style={
        'minWidth': '432px',
        'margin': '0 10px',
        'padding': '15px',
        'border': '1px solid #ddd',
        'borderRadius': '6px',
        'backgroundColor': 'white',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
    })

def create_facets_row(table_name):
    """Create a row of facet boxes for each field in the table"""
    if not table_name:
        return html.Div()
        
    fields_df = get_table_structure(table_name)
    if fields_df.empty:
        return html.Div("No fields found", style={'color': 'red'})
    
    # Filter out text fields only (keep string fields)
    fields_df = fields_df[fields_df['Type'] != 'text']
    
    # Get numeric field stats
    numeric_stats = get_numeric_field_stats(table_name, fields_df)
        
    return html.Div([
        html.Div([
            create_facet_box(
                field_info=field,
                numeric_stats=numeric_stats.get(field['Field']) if field['Type'] in ['bigint', 'timestamp', 'float', 'double'] else None
            ) for field in fields_df.to_dict('records')
        ], style={
            'display': 'flex',
            'overflowX': 'auto',
            'padding': '10px 0',  # Reduced padding
            'gap': '10px'  # Reduced gap
        })
    ], style={
        'height': '1105px',  # 1.7x previous height of 650px
        'width': '100%',
        'backgroundColor': '#f9f9f9',
        'padding': '15px',  # Reduced padding
        'marginBottom': '60px',  # Reduced margin
        'borderRadius': '8px',
        'border': '1px solid #eee',
        'position': 'relative',
        'zIndex': '1'
    })

def create_active_filters_display(filter_states):
    """Create a display of currently active filters"""
    if not filter_states:
        return html.Div("No active filters", style={
            'color': '#666',
            'fontStyle': 'italic',
            'padding': '15px'
        })
    
    filter_cards = []
    for field, state in filter_states.items():
        field_type = state['type']
        values = state['values']
        
        if not values:
            continue
            
        # Create filter card for this field
        filter_cards.append(html.Div([
            # Field name header
            html.H4(field, style={
                'margin': '0 0 8px 0',
                'color': '#333',
                'fontSize': '14px',
                'fontWeight': 'bold',
                'borderBottom': '2px solid #eee',
                'paddingBottom': '4px'
            }),
            # Values list
            html.Div([
                html.Div([
                    # Value text
                    html.Span(
                        value,
                        style={
                            'fontFamily': 'monospace',
                            'fontSize': '13px',
                            'marginRight': '8px',
                            'flex': '1',
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis',
                            'whiteSpace': 'nowrap'
                        }
                    ),
                    # Remove button
                    html.Button(
                        "×",  # Using × character for X
                        id={
                            'type': 'remove-filter',
                            'field': field,
                            'value': value,
                            'field_type': field_type
                        },
                        style={
                            'border': 'none',
                            'background': 'none',
                            'color': '#999',
                            'fontSize': '16px',
                            'cursor': 'pointer',
                            'padding': '0 4px',
                            ':hover': {
                                'color': '#ff4444'
                            }
                        }
                    )
                ], style={
                    'display': 'flex',
                    'alignItems': 'center',
                    'padding': '4px 0',
                    'borderBottom': '1px solid #f0f0f0'
                }) for value in values
            ], style={
                'maxHeight': '120px',  # Limit height to show ~4-5 values
                'overflowY': 'auto'
            })
        ], style={
            'backgroundColor': 'white',
            'border': '1px solid #ddd',
            'borderRadius': '6px',
            'padding': '12px',
            'minWidth': '200px',
            'maxWidth': '100%',
            'boxShadow': '0 1px 3px rgba(0,0,0,0.1)'
        }))
    
    return html.Div([
        html.H3("Active Filters", style={
            'margin': '0 0 15px 0',
            'color': '#333',
            'fontSize': '16px'
        }),
        html.Div(
            filter_cards,
            style={
                'display': 'grid',
                'gridTemplateColumns': 'repeat(auto-fill, minmax(250px, 1fr))',
                'gap': '12px',
                'alignItems': 'start'
            }
        )
    ], style={
        'padding': '15px',
        'backgroundColor': 'white',
        'borderRadius': '4px',
        'height': '100%'
    })

def create_filters_and_sql_display(sql, query_time_ms=None, filter_states=None):
    """Create a side-by-side display of active filters and SQL query"""
    return html.Div([
        # Left side - Active Filters
        html.Div([
            create_active_filters_display(filter_states)
        ], style={
            'width': '50%',
            'paddingRight': '10px',
            'borderRight': '1px solid #ddd',
            'minHeight': '200px',  # Match SQL display height
            'maxHeight': '200px',
            'overflowY': 'auto'
        }),
        
        # Right side - SQL Query
        html.Div([
            create_sql_query_display(sql, query_time_ms)
        ], style={
            'width': '50%',
            'paddingLeft': '10px'
        })
    ], style={
        'display': 'flex',
        'marginBottom': '15px',
        'backgroundColor': 'white',
        'padding': '15px',
        'border': '1px solid #ddd',
        'borderRadius': '4px'
    })

@callback(
    [Output('manticore-facet-total-matches', 'children'),
     Output('manticore-facet-sql-query', 'data'),
     Output('manticore-facet-facets', 'children'),
     Output('manticore-facet-results', 'children')],
    [Input('manticore-facet-search-input', 'value'),
     Input('manticore-facet-table-selector', 'value'),
     Input('manticore-facet-filter-states', 'data')]
)
def update_search_results(search_query, selected_table, filter_states):
    try:
        if not selected_table:
            return '', None, html.Div(), html.Div()
        
        # Get field information
        fields_df = get_table_structure(selected_table)
        fields_df = fields_df[fields_df['Type'] != 'text']  # Filter out text fields
        
        # Get numeric field stats
        numeric_stats = get_numeric_field_stats(selected_table, fields_df)
        
        # Get file name for the selected table
        with get_client(**CLICKHOUSE_SETTINGS) as client:
            df = client.query_df(
                'SELECT file_name FROM input_tables_summary WHERE table_name = %s',
                parameters=(selected_table,)
            )
            if df.empty:
                return '', None, html.Div(), html.H3('Table not found', style={'color': 'red'})
            file_name = df['file_name'].iloc[0]
        
        # First get the total count with filters
        with manticore_client_data_server() as client:
            # Build count query with same WHERE conditions but without the SELECT list
            if search_query and len(search_query.strip()) > 0:
                count_sql = f"SELECT COUNT(*) as count FROM {selected_table} WHERE match(%s)"
                count_params = (search_query,)
            else:
                count_sql = f"SELECT COUNT(*) as count FROM {selected_table} WHERE 1=1"
                count_params = tuple()
            
            if filter_states:
                conditions = build_filter_conditions(filter_states)
                if conditions:
                    count_sql += "\nAND " + "\nAND ".join(conditions)
            
            count_df = manticore_query(client, count_sql, count_params)
            total_count = count_df.iloc[0]['count'] if not count_df.empty else 0
        
        # Execute search with facets and filters
        t0 = time.time()
        
        # First get the main results
        search_sql = prepare_search_query(selected_table, filter_states, include_filters=True, include_main_results=True, search_query=search_query)
        
        # Then get facets for each field, excluding its own filters
        facet_queries = {}
        if filter_states:
            for field in filter_states.keys():
                facet_queries[field] = prepare_search_query(
                    selected_table, 
                    filter_states, 
                    include_filters=True, 
                    include_main_results=False,
                    exclude_field_from_filters=field,
                    search_query=search_query
                )
        
        with manticore_client_data_server() as client:
            # Get main results
            if search_query and len(search_query.strip()) > 0:
                results = manticore_query(client, search_sql, (search_query,))
            else:
                results = manticore_query(client, search_sql, tuple())
            dt_ms = (time.time() - t0) * 1000
            
            # Get facets for each field
            field_facets = {}
            for field, query in facet_queries.items():
                if search_query and len(search_query.strip()) > 0:
                    field_facets[field] = manticore_query(client, query, (search_query,))
                else:
                    field_facets[field] = manticore_query(client, query, tuple())
            
            # Process results and facets
            if isinstance(results, list) and len(results) > 0:
                results_df = results[0]
                
                # Process facets from additional result sets
                facet_data = {}
                if len(results) > 1:
                    string_fields = fields_df[fields_df['Type'] == 'string']['Field'].tolist()
                    numeric_fields = fields_df[fields_df['Type'].isin(['bigint', 'timestamp', 'float', 'double'])]['Field'].tolist()
                    
                    facet_idx = 1  # Skip first result set (main search results)
                    
                    # Process string facets
                    for field in string_fields:
                        if facet_idx < len(results):
                            try:
                                # If this field has an active filter, use its specific facet query results
                                if field in field_facets and isinstance(field_facets[field], list) and len(field_facets[field]) > facet_idx:
                                    facet_df = field_facets[field][facet_idx]
                                    # Verify the facet data has the required columns
                                    if field in facet_df.columns and 'count(*)' in facet_df.columns:
                                        facet_data[field] = facet_df
                                else:
                                    # Verify the results data has the required columns
                                    if field in results[facet_idx].columns and 'count(*)' in results[facet_idx].columns:
                                        facet_data[field] = results[facet_idx]
                            except Exception as e:
                                print(f"Error processing string facet for field {field}: {e}")
                            facet_idx += 1
                    
                    # Process numeric facets
                    for field in numeric_fields:
                        if facet_idx < len(results):
                            try:
                                # Get the numeric stats for this field
                                stats = numeric_stats.get(field)
                                if stats and stats.get('min') is not None and stats.get('max') is not None:
                                    # If this field has an active filter, use its specific facet query results
                                    if field in field_facets and isinstance(field_facets[field], list) and len(field_facets[field]) > facet_idx:
                                        df = field_facets[field][facet_idx].copy()
                                    else:
                                        df = results[facet_idx].copy()
                                    
                                    # Verify the facet data has the required columns
                                    range_col = f'{field}_range'
                                    if range_col in df.columns and 'count(*)' in df.columns:
                                        # Add min/max values to the facet data
                                        df['min'] = stats['min']
                                        df['max'] = stats['max']
                                        facet_data[field] = df
                            except Exception as e:
                                print(f"Error processing numeric facet for field {field}: {e}")
                            facet_idx += 1
                else:
                    results_df = pd.DataFrame()
                    facet_data = {}
        
        # Create facets with the facet data
        facets = html.Div([
            html.Div([
                create_facet_box(
                    field_info=field,
                    facet_data=facet_data.get(field['Field']),
                    numeric_stats=numeric_stats.get(field['Field']) if field['Type'] in ['bigint', 'timestamp', 'float', 'double'] else None,
                    filter_states=filter_states
                ) for field in fields_df.to_dict('records')
            ], style={
                'display': 'flex',
                'overflowX': 'auto',
                'padding': '10px 0',
                'gap': '10px'
            })
        ], style={
            'width': '100%',
            'backgroundColor': '#f9f9f9',
            'padding': '10px',
            'marginBottom': '20px',
            'borderRadius': '4px',
            'border': '1px solid #eee'
        })
        
        # If no results and we have a search query, show suggestions
        if total_count == 0 and search_query and len(search_query.strip()) > 0:
            suggest_sql = f"CALL SUGGEST('{search_query}', '{selected_table}', 5 as limit)"
            with manticore_client_data_server() as client:
                suggestions_df = manticore_query(client, suggest_sql)
            
            if isinstance(suggestions_df, list) or suggestions_df.empty:
                results_display = [
                    create_filters_and_sql_display(search_sql, dt_ms, filter_states),
                    html.H3('No matches or suggestions found', style={'color': '#666'})
                ]
            else:
                results_display = [
                    create_filters_and_sql_display(search_sql, dt_ms, filter_states),
                    html.H3('No direct matches found', style={'color': '#666', 'marginBottom': '15px'}),
                    create_suggestion_box(suggestions_df)
                ]
        else:
            # Sort by weight for display if we have search results
            if search_query and len(search_query.strip()) > 0:
                results_df = results_df.sort_values('weight', ascending=False)
            results_display = [
                create_filters_and_sql_display(search_sql, dt_ms, filter_states),
                create_highlighted_data_table(
                    results_df,
                    title=f'Result Preview: {file_name} ({selected_table})'
                )
            ]
        
        # Update total matches message based on whether we have a search query
        if search_query and len(search_query.strip()) > 0:
            total_matches_msg = f'Total matches for "{search_query}": {total_count}'
        else:
            total_matches_msg = f'Total records: {total_count}'
        
        return (
            total_matches_msg,
            {
                'sql': search_sql,
                'table': selected_table,
                'query': search_query
            },
            facets,
            html.Div(results_display)
        )
    except Exception as e:
        return "", "", "", create_error_div(e)

def create_suggestion_box(suggestions_df, title="Suggested searches:"):
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
                id={'type': 'suggestion-button', 'index': i},
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

@callback(
    Output('manticore-facet-search-input', 'value'),
    Output('manticore-facet-suggestion-click', 'data'),
    [Input({'type': 'suggestion-button', 'index': ALL}, 'n_clicks')],
    [State({'type': 'suggestion-button', 'index': ALL}, 'children'),
     State('manticore-facet-suggestion-click', 'data')]
)
def handle_suggestion_click(clicks, children, prev_click_data):
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
    
    # Return the suggestion to update the input box and store the click
    return suggestion, suggestion

@callback(
    Output('manticore-facet-filter-states', 'data'),
    [Input({'type': 'facet-filter', 'field': ALL, 'value': ALL, 'field_type': ALL}, 'value'),
     Input({'type': 'remove-filter', 'field': ALL, 'value': ALL, 'field_type': ALL}, 'n_clicks')],
    [State({'type': 'facet-filter', 'field': ALL, 'value': ALL, 'field_type': ALL}, 'id'),
     State({'type': 'remove-filter', 'field': ALL, 'value': ALL, 'field_type': ALL}, 'id'),
     State('manticore-facet-filter-states', 'data')]
)
def update_filter_states(checkbox_values, remove_clicks, checkbox_ids, remove_ids, current_states):
    ctx = callback_context
    if not ctx.triggered:
        return current_states or {}
    
    # Initialize states if None
    if not current_states:
        current_states = {}
        
    # Get the trigger ID
    trigger = ctx.triggered[0]['prop_id']
    
    if 'facet-filter' in trigger:
        # Handle checkbox changes
        if not checkbox_values or not checkbox_ids:
            return {}
        
        # Update states based on checkbox changes
        for checkbox_id, value in zip(checkbox_ids, checkbox_values):
            field = checkbox_id['field']
            field_value = checkbox_id['value']
            field_type = checkbox_id['field_type']
            
            # Initialize field in states if not present
            if field not in current_states:
                current_states[field] = {'type': field_type, 'values': []}
            
            # Update values list based on checkbox state
            if value and field_value not in current_states[field]['values']:
                current_states[field]['values'].append(field_value)
            elif not value and field_value in current_states[field]['values']:
                current_states[field]['values'].remove(field_value)
                
            # Remove field if no values are selected
            if field in current_states and not current_states[field]['values']:
                current_states.pop(field)
                
    elif 'remove-filter' in trigger:
        # Handle remove button clicks
        if not remove_clicks or not remove_ids:
            return current_states
            
        # Find which remove button was clicked
        for i, clicks in enumerate(remove_clicks):
            if clicks is not None:  # This button was clicked
                remove_id = remove_ids[i]
                field = remove_id['field']
                value = remove_id['value']
                
                # Remove the value from the filter states
                if field in current_states and value in current_states[field]['values']:
                    current_states[field]['values'].remove(value)
                    # Remove the field if no values left
                    if not current_states[field]['values']:
                        current_states.pop(field)
    
    return current_states 