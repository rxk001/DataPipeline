import sqlite3
from datetime import datetime

import pandas as pd
from Utils import db
from Utils.db import state_lookup
from Utils.log_utils import log_time

STATS_TABLE_NAME = "load_statistics"
STATS_TABLE_COLUMN = ['job_name', 'run_id', 'file_name', 'load_dir', 'insert_dt', 'update_dt',
                      'table_name', 'column_count', 'row_count']

SCHOOL_MATH_RESULT_RAW_TABLE_COLUMN = ['dbn', 'school_name', 'grade', 'year', 'category',
                                       'number_tested', 'mean_scale_score',
                                       'level1_n', 'level1_prcnt', 'level2_n', 'level2_prcnt',
                                       'level3_n', 'level3_prcnt', 'level4_n', 'level4_prcnt',
                                       'level34_n', 'level34_prcnt']

SCHOOL_MATH_RESULT_TABLE_NAME = "school_math_score_result_enriched"
SCHOOL_MATH_RESULT_TABLE_COLUMN = ['school_name', 'grade', 'year', 'category',
                                   'number_tested', 'mean_scale_score']

SCHOOL_MATH_SUMMARY_TABLE_NAME = "school_math_score_result_summary"
SCHOOL_MATH_SUMMARY_TABLE_COLUMN = ['school_name', 'grade', 'year',
                                    'number_tested', 'mean_scale_score']

raw_table_name = {"ny_school_math_score_result": "ny_school_math_score_result_raw",
                  "ct_school_math_score_result": "ct_school_math_score_result_raw"}


def create_stats_table():
    try:
        conn = db.create_DB_connection()
        now = datetime.now()
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        query = (f"CREATE TABLE IF NOT EXISTS {STATS_TABLE_NAME} "
                 f"(job_name string, run_id integer, file_name string, "
                 f"load_dir string, insert_dt string, update_dt string, "
                 f"table_name string, column_count integer, row_count integer)")

        conn.execute(query)

    except sqlite3.Error as error:
        print(f"Failed to Create table {STATS_TABLE_NAME}", error)
    finally:
        if conn:
            db.close_connection(conn)


@log_time
def validate_stats(job_name, run_id):
    try:
        conn = db.create_DB_connection()
        query = f"SELECT count(*) FROM {STATS_TABLE_NAME} WHERE job_name = '{job_name}' and run_id = {run_id}"
        cursor = conn.execute(query)
        count_out = cursor.fetchone()[0]

        if count_out > 0:
            error = f"Job: {job_name} with run_id: {run_id} already exists. Existing"
            raise NameError(error)

    except sqlite3.Error as error:
        print("Select failed", error)
    finally:
        if conn:
            db.close_connection(conn)
    return


@log_time
def validate_enriched(run_flag, run_id):
    validate_run(SCHOOL_MATH_RESULT_TABLE_NAME, run_flag, run_id)


@log_time
def validate_summary(run_flag, run_id):
    validate_run(SCHOOL_MATH_SUMMARY_TABLE_NAME, run_flag, run_id)


def get_state(run_flag):
    if run_flag not in state_lookup:
        raise NameError(f"No state setup fot the run: {run_flag}. Existing")
    return state_lookup[run_flag]

@log_time
def validate_run(table_name, run_flag, run_id):
    state = get_state(run_flag)
    try:
        conn = db.create_DB_connection()
        query = f"SELECT count(*) FROM {table_name} WHERE run_id = {run_id} AND state = {state}"
        cursor = conn.execute(query)
        count_out = cursor.fetchone()[0]

        if count_out > 0:
            error = f"run_id: {run_id} for  state: {state} already exists in {table_name}. Existing"
            raise NameError(error)

    except sqlite3.Error as error:
        print("Select failed", error)
    finally:
        if conn:
            db.close_connection(conn)
    return


@log_time
def insert_run_stats(job_name, run_id, file_name, load_dir):
    create_stats_table()
    try:
        conn = db.create_DB_connection()
        now = datetime.now()
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        query = f"INSERT INTO {STATS_TABLE_NAME} " \
                f"(job_name, run_id, file_name, load_dir, insert_dt) " \
                f"VALUES (?, ?, ?, ?, ?);"
        data_tuple = (job_name, run_id, file_name, load_dir, current_time)
        conn.execute(query, data_tuple)
    except sqlite3.Error as error:
        print("INSERT INTO {STATS_TABLE_NAME} Filed", error)
    finally:
        if conn:
            db.close_connection(conn)


@log_time
def update_run_stats(job_name, run_id, table_name, column_count, row_count):
    try:
        conn = db.create_DB_connection()
        now = datetime.now()
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        query = (f"UPDATE {STATS_TABLE_NAME} "
                 f"SET update_dt = '{current_time}', "
                 f"table_name = '{table_name}', "
                 f"column_count = {column_count}, "
                 f"row_count = {row_count} "
                 f"WHERE job_name = '{job_name}' "
                 f"AND run_id = {run_id}")
        conn.execute(query)

    except sqlite3.Error as error:
        print(f"UPDATE table {STATS_TABLE_NAME} Filed", error)
    finally:
        if conn:
            db.close_connection(conn)


def get_raw_table_name(run_flag):
    return raw_table_name[run_flag]


def create_math_result_table():
    try:
        conn = db.create_DB_connection()
        now = datetime.now()
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        query = f"CREATE TABLE IF NOT EXISTS {SCHOOL_MATH_RESULT_TABLE_NAME} " \
                f"(state string, school_name string, grade string, " \
                f"year integer, category string, number_tested integer, " \
                f"mean_scale_score integer, run_id integer)"
        conn.execute(query)
    except sqlite3.Error as error:
        print(f"Failed to Create table {SCHOOL_MATH_RESULT_TABLE_NAME}", error)
    finally:
        if conn:
            db.close_connection(conn)


def insert_math_result_table(run_flag, run_id):
    table_name = get_raw_table_name(run_flag)
    try:
        conn = db.create_DB_connection()
        now = datetime.now()
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        query = f"INSERT INTO TABLE  {SCHOOL_MATH_RESULT_TABLE_NAME} " \
                f"(school_name, grade, " \
                f"year, category, number_tested, " \
                f"mean_scale_score, run_id) " \
                f"SELECT school_name, grade, " \
                f"year, category, number_tested, " \
                f"mean_scale_score, {run_id} " \
                f"FROM  {table_name} "
        conn.execute(query)
    except sqlite3.Error as error:
        print(f"Failed to Create table {SCHOOL_MATH_RESULT_TABLE_NAME}", error)
    finally:
        if conn:
            db.close_connection(conn)


@log_time
def get_raw_school_math_score(run_flag):
    table_name = get_raw_table_name(run_flag)
    conn = db.create_DB_connection()
    query = (f"SELECT school_name, grade, "
             f"year, category, number_tested, "
             f"mean_scale_score "
             f"FROM  {table_name} ")
    df = pd.read_sql_query(query, conn)
    db.close_connection(conn)
    return df


@log_time
def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)


@log_time
def drop_dups(df):
    """
    Drop Duplicated Records in dataframe
    """
    total_rows_before = df.shape[0]
    df = trim_all_columns(df)
    df.drop_duplicates(inplace=True, keep=False)
    total_rows_after = df.shape[0]
    dups = total_rows_before - total_rows_after
    print(f"Duplicated row(s): {dups} (deleted). ROW COUNT: {total_rows_after}")
    return df


@log_time
def drop_bad_score(df):
    """
    Drop Duplicated Records in dataframe
    """
    total_rows_before = df.shape[0]
    df = trim_all_columns(df)
    df = df[df.mean_scale_score != 's']
    total_rows_after = df.shape[0]
    bad_scores = total_rows_before - total_rows_after
    print(f"Bad mean_scale_score row(s): {bad_scores} (deleted). ROW COUNT: {total_rows_after}")
    return df


@log_time
def enrich_data(df, run_flag, run_id):
    """
    Convert strings to int and add run_id and datetime
    """
    df["number_tested"] = [int(x) for x in df["number_tested"]]
    df["year"] = [int(x) for x in df["year"]]
    df["mean_scale_score"] = [int(x) for x in df["mean_scale_score"]]
    add_state_run_id_dt_data(df, run_flag, run_id)
    return df


def add_state_run_id_dt_data(df, run_flag, run_id):
    """
    Add run_id, datetime
    """
    now = datetime.now()
    current_time = now.strftime("%d-%m-%Y %H:%M:%S")
    state = get_state(run_flag)
    df["state"] = state
    df["run_id"] = run_id
    df["insert_dt"] = current_time



@log_time
def store_df(df, table_name, run_flag, run_id):
    """
    Store data
    """
    db.store_df(df, table_name, run_flag, run_id)


@log_time
def store_enriched_df(df, run_flag, run_id):
    """
    Store enriched data
    """
    store_df(df, SCHOOL_MATH_RESULT_TABLE_NAME, run_flag, run_id)


@log_time
def store_summary_df(df, run_flag, run_id):
    """
    Store summary data
    """
    store_df(df, SCHOOL_MATH_SUMMARY_TABLE_NAME, run_flag, run_id)


@log_time
def get_math_score_enriched(run_id):
    conn = db.create_DB_connection()
    query = (f"SELECT school_name, grade, "
             f"year, category, number_tested, "
             f"mean_scale_score "
             f"FROM  {SCHOOL_MATH_RESULT_TABLE_NAME} "
             f"WHERE run_id = {run_id}")
    df = pd.read_sql_query(query, conn)
    db.close_connection(conn)
    return df


def get_math_score_summary(df):
    school_array = df[df.grade == '8'].school_name.unique()
    df2 = pd.DataFrame(school_array, columns=['school_name'])
    df3 = pd.merge(df, df2, on=["school_name"], how='inner')
    df_final = (df3
                .groupby(["school_name", "grade", "year"], as_index=False)
                .agg({'number_tested': 'sum', 'mean_scale_score': 'sum'})
                .reset_index())
    return df_final
