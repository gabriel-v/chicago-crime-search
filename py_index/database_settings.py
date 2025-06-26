CLICKHOUSE_SETTINGS = {
    'host': 'localhost',
    'user': 'chicago_crimes_search',
    'password': 'chicago_crimes_search',
    'database': 'chicago_crimes_search',
    'settings': {
        'async_insert': 1,
        'wait_for_async_insert': 1,
        'input_format_values_interpret_expressions': 1,
        'input_format_allow_errors_num': 1024,
        'input_format_allow_errors_ratio': 0.05,
    }
}