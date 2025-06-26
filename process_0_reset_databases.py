#!/usr/bin/env python3


from py_index.clickhouse_database_ops import  reset_database, reset_tables


def process_0_reset_databases():
    reset_database()
    reset_tables()
    

if __name__ == "__main__":
    process_0_reset_databases() 