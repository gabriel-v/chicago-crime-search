from dash import html, dash_table
import pandas as pd
import traceback

def create_data_table(df, title=None, page_size=100):
    """
    Creates a styled DataTable component for displaying pandas DataFrames
    
    Args:
        df: pandas DataFrame to display
        title: Optional title to show above the table
        page_size: Number of rows per page (default 20)
    
    Returns:
        html.Div containing the table and optional title
    """
    components = []
    
    if title:
        components.append(html.H3(title))
    
    components.append(
        dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': col, 'id': col} for col in df.columns],
            style_table={'overflowX': 'auto'},
            style_header={
                'backgroundColor': '#C2D4FF',
                'fontWeight': 'bold',
                'textAlign': 'left'
            },
            style_cell={
                'textAlign': 'left',
                'padding': '10px'
            },
            page_size=page_size
        )
    )
    
    return html.Div(components)

def create_sql_query_display(sql, query_time_ms=None):
    """Create a SQL query display box with optional query time"""
    return html.Div([
        html.Div([
            html.H4('SQL Query:', style={'marginBottom': '5px', 'display': 'inline-block'}),
            html.H4(f'{query_time_ms:.1f}ms' if query_time_ms is not None else '', style={
                'float': 'right',
                'margin': '0',
                'color': '#666',
                'fontWeight': 'normal'
            })
        ]),
        html.Div([
            html.Pre(
                sql,
                style={
                    'margin': '0',
                    'backgroundColor': '#f5f5f5',
                    'padding': '15px',
                    'borderRadius': '8px',
                    'fontFamily': 'monospace',
                    'whiteSpace': 'pre-wrap',
                    'wordBreak': 'break-word',
                    'fontSize': '14px',
                    'position': 'relative',
                    'zIndex': '0'
                }
            )
        ], style={
            'maxHeight': '200px',
            'overflowY': 'auto',
            'border': '1px solid #eee',
            'borderRadius': '8px'
        })
    ])

def create_facet_table(data, field_name, field_type):
    """Create a custom HTML table for facet data with checkboxes for WHERE clause filtering"""
    if data.empty:
        return html.Div("No data", style={'color': '#666', 'fontStyle': 'italic'})

    # Create table rows
    rows = []
    for idx, row in data.iterrows():
        # Get the value and count from the row
        if field_type in ['bigint', 'timestamp', 'float', 'double']:
            # For numeric ranges, use the 'range' column
            value = row['range']
            count = row['count']
        else:
            # For string fields, first column is the value, second is count
            value = row[field_name]
            count = row['count(*)']

        # Create row with value, count, and checkbox
        rows.append(
            html.Tr([
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
                    str(count),
                    style={
                        'padding': '6px',
                        'borderBottom': '1px solid #eee',
                        'fontFamily': 'monospace',
                        'fontSize': '14px',
                        'textAlign': 'right',
                        'width': '60px'
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
                        value=[],
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
            ], style={
                'backgroundColor': 'white',
                'transition': 'background-color 0.2s',
                ':hover': {
                    'backgroundColor': '#f5f5f5'
                }
            })
        )

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

def create_custom_data_table(df, title):
    """Create a custom data table with basic styling"""
    return html.Div([
        html.H4(title),
        dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns],
            page_size=100,
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '8px',
                'fontFamily': 'monospace'
            },
            style_header={
                'backgroundColor': '#f1f8ff',
                'fontWeight': 'bold',
                'border': '1px solid #ddd'
            },
            style_data={
                'border': '1px solid #ddd'
            }
        )
    ])

def create_highlighted_data_table(df, title):
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
                        # Special handling for highlight_all column
                        highlight_text_to_spans(str(cell)) if col == 'highlight_all' and isinstance(cell, str) and "__before__" in str(cell)
                        else html.Div(
                            str(cell) if not pd.isna(cell) else '(null)',
                            style={'fontFamily': 'monospace'}
                        ),
                        style={
                            'padding': '8px',
                            'border': '1px solid #ddd',
                            'whiteSpace': 'pre-wrap',
                            'fontFamily': 'monospace',
                            'maxWidth': '400px',  # Limit cell width
                            'overflow': 'hidden',
                            'textOverflow': 'ellipsis'
                        }
                    )
                    for col, cell in row.items()
                ],
                style={
                    'backgroundColor': 'white',
                    'borderBottom': '1px solid #ddd',
                    ':hover': {'backgroundColor': '#f5f5f5'}
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

def highlight_text_to_spans(text):
    """Convert custom highlight markers to styled spans"""
    parts = []
    current_pos = 0
    
    while True:
        start = text.find("__before__", current_pos)
        if start == -1:
            # Add remaining text if any
            if current_pos < len(text):
                parts.append(html.Span(text[current_pos:], style={
                    'whiteSpace': 'pre-wrap',
                    'wordBreak': 'break-word',
                    'overflowWrap': 'break-word'
                }))
            break
            
        end = text.find("__after__", start)
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
        highlighted_text = text[start + len("__before__"):end]
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
        
        current_pos = end + len("__after__")
    
    return parts 

def create_error_div(e):
    """Helper function to create a standardized error display with full traceback
    
    Args:
        e: The exception object or error message string
    """
    return html.Div([
        html.H4("Error:", style={'color': 'red', 'marginBottom': '10px'}),
        html.Pre(str(e), style={'color': 'red', 'marginBottom': '20px'}),
        html.H4("Full traceback:", style={'color': 'red', 'marginBottom': '10px'}),
        html.Pre(traceback.format_exc(), style={
            'backgroundColor': '#f8f8f8',
            'padding': '10px',
            'border': '1px solid #ddd',
            'borderRadius': '4px',
            'whiteSpace': 'pre-wrap',
            'wordWrap': 'break-word',
            'color': '#c41e3a',  # Dark red color for traceback
            'fontFamily': 'monospace'
        })
    ]) 