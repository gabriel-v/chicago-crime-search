#!/usr/bin/env python3

import re
import os
import glob
from datetime import datetime
import xml.etree.ElementTree as ET
import pandas as pd
from clickhouse_connect import get_client
from py_index.clickhouse_database_ops import execute_query, fetch_table_raw_column_stats, recreate_table
from py_index.database_settings import CLICKHOUSE_SETTINGS
from py_index.manticore_database_ops import index_table_into_manticore
import csv



def process_1_input_clickhouse():
    file_list = glob.glob('docker/data/**/*.csv', recursive=True)
    file_list.extend(glob.glob('docker/data/**/*.xml', recursive=True))
    # sort by file size increasing
    file_list.sort(key=lambda x: os.path.getsize(x))
    with get_client(**CLICKHOUSE_SETTINGS) as client:
        existing_filenames_df =  client.query_df('select file_name from input_tables_list')
        if existing_filenames_df.empty:
            existing_filenames = set()
        else:
            existing_filenames = set(existing_filenames_df['file_name'].tolist())

    # Process each CSV file
    for (i, filepath) in enumerate(file_list):
        file_size = os.path.getsize(filepath)
        filename = os.path.basename(filepath)
        if filename in existing_filenames:
            print('Skipping already ingested file', filename)
            continue

        _, extension = os.path.splitext(filename)
        table_name = None
        if extension == '.csv':
            table_name = ingest_csv_file(i, filepath, filename, file_size)
        elif extension == '.xml':
            table_name = ingest_wiki_xml_file(i, filepath, filename, file_size)

        if table_name is None:
            print('Error loading file', filename)
            continue
        print('Done loading file', filename, 'as', table_name)
        fetch_table_raw_column_stats(table_name)
        old_name = table_name
        table_name = recreate_table(table_name)
        if table_name is None:
            print('Error recreating table', old_name)
            continue
        print('Done recreating table', table_name)
        index_table_into_manticore(table_name)
        print('Done indexing table', table_name)

def ingest_wiki_xml_file(file_index, filepath, filename, file_size):
    file_stem = '_'.join(os.path.splitext(filename)[:-1])
    # remove all non_alphanumeric characters
    file_stem = re.sub(r'[^a-zA-Z0-9]', ' ', file_stem.lower()).replace('  ', ' ').strip().replace(' ', '_')[:16]
    file_idx_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    item_name = f"{str(file_index).zfill(3)}_{file_stem}_{file_idx_time}"
    table_name = f"_input_log_{item_name}"
    print(f"Loading XML {filename} into Clickhouse as {table_name}")

    # Define schema based on expected Wikipedia XML structure
    columns = {
        'title': 'String',
        'ns': 'UInt64',
        'id': 'UInt64',
        'revision_id': 'UInt64',
        'revision_parent_id': 'Nullable(UInt64)',
        'revision_timestamp': 'Nullable(DateTime)',
        'contributor_username': 'Nullable(String)',
        'contributor_id': 'Nullable(UInt64)',
        'comment': 'Nullable(String)',
        'model': 'Nullable(String)',
        'format': 'Nullable(String)',
        'text': 'String'
    }
    col_defs = ', '.join([f'`{k}` {v}' for k, v in columns.items()])

    with get_client(**CLICKHOUSE_SETTINGS) as client:
        try:
            # Create table
            create_table_query = f"CREATE TABLE {table_name} ({col_defs}) ENGINE = Log"
            execute_query(client, create_table_query)

            # Use iterparse for streaming to handle large files
            context = ET.iterparse(filepath, events=('start', 'end'))

            # Get the root element to dynamically determine the namespace
            _, root = next(context)
            namespace = ''
            if '}' in root.tag:
                namespace = root.tag.split('}')[0][1:]

            # Prepare namespace for find operations
            namespace_str = f"{{{namespace}}}" if namespace else ""

            rows = []

            for event, elem in context:
                if event == 'end' and elem.tag == f'{namespace_str}page':
                    page_data = {}

                    # Extract revision info
                    revision = elem.find(f'{namespace_str}revision')
                    if revision is not None:
                        rev_id_elem = revision.find(f'{namespace_str}id')
                        page_data['revision_id'] = int(rev_id_elem.text) if rev_id_elem is not None and rev_id_elem.text else 0

                        rev_parent_id_elem = revision.find(f'{namespace_str}parentid')
                        page_data['revision_parent_id'] = int(rev_parent_id_elem.text) if rev_parent_id_elem is not None and rev_parent_id_elem.text else None

                        rev_timestamp_elem = revision.find(f'{namespace_str}timestamp')
                        page_data['revision_timestamp'] = rev_timestamp_elem.text if rev_timestamp_elem is not None else None

                        text_elem = revision.find(f'{namespace_str}text')
                        page_data['text'] = text_elem.text if text_elem is not None and text_elem.text is not None else ''

                        model_elem = revision.find(f'{namespace_str}model')
                        page_data['model'] = model_elem.text if model_elem is not None and model_elem.text is not None else None

                        format_elem = revision.find(f'{namespace_str}format')
                        page_data['format'] = format_elem.text if format_elem is not None and format_elem.text is not None else None

                        contributor = revision.find(f'{namespace_str}contributor')
                        if contributor is not None:
                            username_elem = contributor.find(f'{namespace_str}username')
                            page_data['contributor_username'] = username_elem.text if username_elem is not None else None

                            contrib_id_elem = contributor.find(f'{namespace_str}id')
                            page_data['contributor_id'] = int(contrib_id_elem.text) if contrib_id_elem is not None and contrib_id_elem.text else None
                        else:
                            page_data['contributor_username'] = None
                            page_data['contributor_id'] = None

                        comment_elem = revision.find(f'{namespace_str}comment')
                        page_data['comment'] = comment_elem.text if comment_elem is not None else None
                    else:
                        page_data['revision_id'] = 0
                        page_data['revision_parent_id'] = None
                        page_data['revision_timestamp'] = None
                        page_data['text'] = ''
                        page_data['model'] = None
                        page_data['format'] = None
                        page_data['contributor_username'] = None
                        page_data['contributor_id'] = None
                        page_data['comment'] = None

                    title_elem = elem.find(f'{namespace_str}title')
                    page_data['title'] = title_elem.text if title_elem is not None else ''

                    ns_elem = elem.find(f'{namespace_str}ns')
                    page_data['ns'] = int(ns_elem.text) if ns_elem is not None and ns_elem.text else 0

                    id_elem = elem.find(f'{namespace_str}id')
                    page_data['id'] = int(id_elem.text) if id_elem is not None and id_elem.text else 0

                    rows.append(page_data)

                    if len(rows) >= 8192:
                        df = pd.DataFrame(rows)
                        df['revision_timestamp'] = pd.to_datetime(df['revision_timestamp'], errors='coerce')
                        for col in ['revision_parent_id', 'contributor_id']:
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        client.insert_df(table_name, df)
                        rows = []

                    # Clear the element to free memory
                    elem.clear()

            root.clear()

            if rows:
                df = pd.DataFrame(rows)
                df['revision_timestamp'] = pd.to_datetime(df['revision_timestamp'], errors='coerce')
                for col in ['revision_parent_id', 'contributor_id']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                client.insert_df(table_name, df)

            # Insert into input_tables_list
            execute_query(client,
                f"INSERT INTO input_tables_list (table_name, file_name, item_name, event_time, file_size) VALUES ('{table_name}', '{filename}', '{item_name}', NOW(), {file_size})"
            )
            return table_name

        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            execute_query(client, f"DROP TABLE IF EXISTS {table_name} SYNC;")
            return None
        finally:
            # Ensure the reference is cleared in case of an error
            if 'elem' in locals() and elem is not None:
                elem.clear()

def do_ingest_csv_file_fallback(client, table_name, csv_path):
    print(f"Falling back to Python CSV parser for {csv_path}")
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
            try:
                dialect = csv.Sniffer().sniff(csvfile.read(8192))
                print(f"Sniffed CSV dialect: delimiter='{dialect.delimiter}', quotechar='{dialect.quotechar}'")
            except csv.Error:
                dialect = csv.excel
                dialect.delimiter = ','
                print("CSV sniffing failed, defaulting to comma delimiter.")

            csvfile.seek(0)
            reader = csv.reader(csvfile, dialect)

            try:
                header = next(reader)
            except StopIteration:
                print(f"CSV file {csv_path} is empty. Skipping.")
                return

            sanitized_columns = []
            for col in header:
                s_col = re.sub(r'[^a-zA-Z0-9_]', '_', col).strip()
                if not s_col or s_col[0].isdigit():
                    s_col = '_' + s_col
                sanitized_columns.append(s_col)

            col_defs = ', '.join([f'`{col}` Nullable(String)' for col in sanitized_columns])
            create_table_query = f"CREATE TABLE {table_name} ({col_defs}) ENGINE = Log"
            execute_query(client, create_table_query)

            csvfile.seek(0)
            next(reader)  # Skip header again

            rows_to_insert = []
            total_row_count = 0
            error_count = 0

            for line in csvfile:
                total_row_count += 1
                try:
                    row = next(csv.reader([line], dialect))

                    if len(row) > len(sanitized_columns):
                        row = row[:len(sanitized_columns)]
                    elif len(row) < len(sanitized_columns):
                        row.extend([None] * (len(sanitized_columns) - len(row)))

                    rows_to_insert.append(row)

                    if len(rows_to_insert) >= 8192:
                        df = pd.DataFrame(rows_to_insert, columns=sanitized_columns)
                        client.insert_df(table_name, df)
                        rows_to_insert = []
                except csv.Error as e:
                    error_count += 1
                    print(f"Warning: Skipping malformed row #{total_row_count + 1} in {csv_path}. Error: {e}")
                    if total_row_count > 200 and (error_count / total_row_count) > 0.05:
                        raise Exception(f"Aborting due to excessive parsing errors in {csv_path}.") from e
                    continue

            if rows_to_insert:
                df = pd.DataFrame(rows_to_insert, columns=sanitized_columns)
                client.insert_df(table_name, df)

            print(f"Fallback ingestion for {csv_path} complete. Total rows: {total_row_count}, Skipped rows: {error_count}")

    except csv.Error as e:
        print(f"CSV parsing error in {csv_path}: {e}")
        execute_query(client, f"DROP TABLE IF EXISTS {table_name} SYNC;")
        raise e
    except Exception as e:
        print(f"Fallback CSV ingestion failed for {csv_path}: {e}")
        execute_query(client, f"DROP TABLE IF EXISTS {table_name} SYNC;")
        raise e

def ingest_csv_file(file_index, filepath, filename, file_size):
    full_filepath = os.path.realpath(filepath)
    relative_filepath = os.path.relpath(full_filepath, os.path.realpath('docker/data'))

    file_stem = os.path.splitext(filename)[0]
    # remove all non_alphanumeric characters
    file_stem = re.sub(r'[^a-zA-Z0-9]', ' ', file_stem.lower()).replace('  ', ' ').strip().replace(' ', '_')[:16]
    file_idx_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    item_name = f"{str(file_index).zfill(3)}_{file_stem}_{file_idx_time}"
    table_name = f"_input_log_{item_name}"
    print(f"Loading CSV {filename} into Clickhouse as {table_name}")

    with get_client(**CLICKHOUSE_SETTINGS) as client:
        try:
            try:
                # Configure and create table from CSV
                execute_query(client, f'''
                CREATE TABLE {table_name} ENGINE = Log AS SELECT * FROM file('{relative_filepath}', CSVWithNames)
                ''')
            except Exception as e:
                print(f"ClickHouse failed to parse CSV directly, falling back to Python parser: {e}")
                # drop the empty table that might have been created
                execute_query(client, f"DROP TABLE IF EXISTS {table_name} SYNC;")
                do_ingest_csv_file_fallback(client, table_name, full_filepath)


            # Insert into input_tables_list
            execute_query(client,
                f"INSERT INTO input_tables_list (table_name, file_name, item_name, event_time, file_size) VALUES ('{table_name}', '{filename}', '{item_name}', NOW(), {file_size})"
            )
            return table_name
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            execute_query(client, f"DROP TABLE IF EXISTS {table_name} SYNC;")
            return None



if __name__ == "__main__":
    process_1_input_clickhouse()