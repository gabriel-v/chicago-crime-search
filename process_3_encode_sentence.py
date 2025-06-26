from concurrent.futures import ThreadPoolExecutor
import clickhouse_connect
import pandas as pd
import time
from py_index.database_settings import CLICKHOUSE_SETTINGS
from py_index.manticore_database_ops import manticore_client_weights_server, manticore_query
import datetime


MIN_TEXT_LENGTH = 16
CHUNK_SIZE = 2048

from model2vec import StaticModel
model = StaticModel.from_pretrained("minishlab/potion-base-2M")


def load_text(table_name):
    try:
        yield from load_text_from_table(table_name)
    except Exception as e:
        print(f"Error loading text from table {table_name}: {e}")


def load_text_from_table(table_name):
    with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as client:
        table_columns = client.query_df(f"""
            SELECT name FROM system.columns
            WHERE table = '{table_name}'
            and database = 'chicago_crimes_search'
            and type in ('String', 'LowCardinality(String)', 'Nullable(String)')
        """)
        if table_columns.empty:
            return
        table_columns = table_columns['name'].tolist()

        sql = f"""
        SELECT id, concatWithSeparator(' ', {', '.join(f"CASE WHEN {col} IS NULL THEN '' ELSE {col} END" for col in table_columns)}) as text FROM {table_name}
        WHERE ({' + '.join(f"CASE WHEN {col} IS NULL THEN 0 ELSE length(trim({col})) END" for col in table_columns)}) >= {MIN_TEXT_LENGTH}
        ORDER BY id ASC
        """
        with client.query_df_stream(sql) as data_stream:
            for data in data_stream:
                if data.empty:
                    break

                new_rows = []
                for _, row in data.iterrows():
                    text = row['text']
                    chunks = [text[i:i+CHUNK_SIZE] for i in range(0, len(text), CHUNK_SIZE)]
                    for chunk in chunks:
                        if len(chunk.strip()) >= MIN_TEXT_LENGTH:
                            new_rows.append({'id': row['id'], 'text': chunk})

                if not new_rows:
                    continue

                chunked_df = pd.DataFrame(new_rows)
                df_size = sum(chunked_df['text'].apply(lambda x:  len(x) if bool(x) else 16))
                yield (table_name, chunked_df, df_size)


def embed(data):
    data = data['text'].tolist()
    data_len = sum(len(x) for x in data)
    t0 = time.time()
    embeddings = model.encode(data)
    t1 = time.time()
    print(f"Encoded {data_len} characters in {t1-t0} seconds = {data_len/(t1-t0)/1024/1024} MB/s")
    embeddings = [list(i) for i in embeddings]
    return embeddings


def insert_data_into_weights_table(table_name, data2, embeddings):
    t0_total = time.time()
    total_bytes = 0
    batch_size = 10000

    # Create a list of tuples, each containing a row's data and its corresponding embedding.
    # This assumes data2 and embeddings are aligned.
    rows_with_embeddings = list(zip(data2.to_dict('records'), embeddings))

    for i in range(0, len(rows_with_embeddings), batch_size):
        batch = rows_with_embeddings[i:i + batch_size]

        value_clauses = []
        params = []

        for row_dict, embedding in batch:
            vector_str = ','.join(map(str, embedding))

            # The vector part is embedded directly into the query string because Manticore's tuple syntax `(1,2,3)`
            # is not a standard SQL type that drivers understand for parameterization.
            # The text_str part, however, IS parameterized to prevent SQL injection.
            value_clause = f"('{table_name}', {row_dict['id']}, %s, ({vector_str}))"
            value_clauses.append(value_clause)
            params.append(row_dict['text'])

        if not value_clauses:
            continue

        sql_values_part = ", ".join(value_clauses)
        sql = f"""
        INSERT INTO text_vector_64_floats (table_name, table_rowid, text_str, text_vector)
        VALUES {sql_values_part}
        """

        with manticore_client_weights_server() as client:

            manticore_query(client, sql, params)
            total_bytes += len(sql) + sum(len(p) for p in params)

            manticore_query(client, "COMMIT")

    t1_total = time.time()
    duration = t1_total - t0_total
    total_rows_inserted = len(rows_with_embeddings)
    print(f"Inserted {total_rows_inserted} rows ({total_bytes/1024/1024:.2f} MB) in {duration:.2f} seconds = {total_bytes/duration/1024/1024 if duration > 0 else 0:.2f} MB/s")



def process_table_compute_upload_vectors(table_name):
    current_size = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for (_, data2, df_size) in load_text(table_name):
            msg = f"Chunk in: {df_size/1024} KB"
            current_size += df_size
            embeddings = embed(data2)
            msg += f" - Chunk out: {sum(len(x) for x in embeddings)/1024} KB"
            futures.append(executor.submit(insert_data_into_weights_table, table_name, data2, embeddings))
            current_speed_kb = current_size / (time.time() - t0) / 1024
            print(f"  - Current speed: {current_speed_kb} KB/s - {msg}")

        for future in futures:
            future.result()

    with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as c:
        current_time = datetime.datetime.now()
        c.insert('input_table_vectors_computed', [[table_name, current_time]], column_names=['table_name', 'event_time'])



def process_all_tables_upload_vectors():
    init_various_tables()
    with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as client:
        all_tables_df = client.query_df("SELECT table_name FROM input_tables_summary ORDER BY table_name")
        data = all_tables_df['table_name'].tolist()

        completed_tables_df = client.query_df("SELECT DISTINCT table_name FROM input_table_vectors_computed")
        if not completed_tables_df.empty:
            completed_tables = set(completed_tables_df['table_name'].tolist())
            data = [table for table in data if table not in completed_tables]

    if not data:
        print("All tables have already been processed.")
        return

    # run in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_table_compute_upload_vectors, table_name) for table_name in data]
        for future in futures:
            future.result()


def init_various_tables():
    with manticore_client_weights_server() as client:
        tables = manticore_query(client, "SHOW TABLES")
        if not tables.empty:
            tables = tables['Table'].tolist()
        else:
            tables = []
        if 'text_vector_64_floats' not in tables:
            manticore_query(
                client,
            f"""
            CREATE TABLE text_vector_64_floats (
                table_name string attribute,
                table_rowid bigint,
                text_str text,
                text_vector float_vector knn_type='hnsw' knn_dims='64' hnsw_similarity='l2'
            )
            """)

    with clickhouse_connect.get_client(**CLICKHOUSE_SETTINGS) as c:
        c.command("""CREATE TABLE IF NOT EXISTS input_table_vectors_computed (
            table_name String,
            event_time DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (table_name, event_time)
        """)


if __name__ == "__main__":
    process_all_tables_upload_vectors()