import sqlite3
from pandas.io import sql
import pandas as pd

DB_NAME = "db.sqlite"

state_lookup = {"ny_school_math_score_result": "NY",
                "ct_school_math_score_result": "CT"}


def create_DB_connection():
    """ Connect to a database and create cursor object for executing SQL statements.
    """
    connection = sqlite3.connect(DB_NAME, uri=True)
    # print("Connection with database established ..........")
    connection.cursor()
    return connection


def close_connection(connection):
    """ Close connection with the database.
    """
    connection.commit()
    connection.close()


def drop_table(table):
    conn = sqlite3.connect(DB_NAME)
    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.close()


def store_df(df, table_name_out):
    """
    Store DataFrame
    """
    conn = create_DB_connection()
    sql.to_sql(df,
               name=table_name_out,
               con=conn,
               index=False,  # don't use CSV file index
               # index_label='molecule_id', # use a unique column from DataFrame as index
               if_exists='append')

    query = f"SELECT count(*) from {table_name_out} "
    cursor = conn.execute(query)
    count_out = cursor.fetchone()[0]

    count_in = df.shape[0]
    close_connection(conn)

    if count_in != count_out:
        print(f"ERROR: Read: {count_in}, Insert: {count_out} Records")
        exit(1)
    else:
        print(f"Success: Read: {count_in}, Insert: {count_out} Records")


def store_df(df, table_name_out, run_flag, run_id):
    """
    Store DataFrame
    """
    state = state_lookup[run_flag]
    conn = create_DB_connection()
    sql.to_sql(df,
               name=table_name_out,
               con=conn,
               index=False,  # don't use CSV file index
               # index_label='molecule_id', # use a unique column from DataFrame as index
               if_exists='append')

    query = f"SELECT count(*) from {table_name_out} WHERE run_id = {run_id} AND state = '{state}'"
    cursor = conn.execute(query)
    count_out = cursor.fetchone()[0]

    count_in = df.shape[0]
    close_connection(conn)

    if count_in != count_out:
        print(f"ERROR: Read: {count_in}, Insert: {count_out} Records")
        exit(1)
    else:
        print(f"Success: Read: {count_in}, Insert: {count_out} Records")


def select(table_name):
    """
    Reads data into dataframe
    """
    query = f"SELECT * FROM {table_name}"
    conn = create_DB_connection()
    df = pd.read_sql(query, conn)
    return df



