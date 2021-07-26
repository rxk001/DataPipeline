from pathlib import Path

import pandas as pd
from pandas.io import sql
import os
from Utils import db
from Utils.log_utils import log_time

from Utils.pipeline_utils import get_raw_table_info, get_input_file_details
from Utils.sql_utils import update_run_stats

path2data = os.getcwd()
input_dir = path2data + "\\data\\"

@log_time
def store_csv(table_name, columns, run_flag):
    """
    Store csv file as is
    """
    db.drop_table(table_name)
    conn = db.create_DB_connection()
    chunksize = 1000  # number of lines to process at each iteration

    url, file_name = get_input_file_details(run_flag)
    in_csv = input_dir + file_name

    num_lines = sum(1 for line in open(in_csv))
    num_lines -= 1  # without header

    for i in range(1, num_lines, chunksize):  # change 0 -> 1 if your csv file contains a column header
        print(f".... Reading {i} out of {num_lines} records")
        df = pd.read_csv(in_csv,
                         header=None,  # no header, define column header manually later
                         nrows=chunksize,  # number of rows to read at each iteration
                         skiprows=i,
                         encoding="ISO-8859-1",  # Windows Encoding
                         engine='python')  # skip rows that were already read

        df.columns = columns

        sql.to_sql(df,
                   name=table_name,
                   con=conn,
                   index=False,  # don't use CSV file index
                   # index_label='molecule_id', # use a unique column from DataFrame as index
                   if_exists='append')

    # Validate Insert
    query = f"SELECT count(*) from {table_name}"
    cursor = conn.execute(query)
    row_count = cursor.fetchone()[0]

    if row_count != num_lines:
        print(f"ERROR: Read: {num_lines}, insert: {row_count} Records (header skipped) into table: {table_name}")
        exit(1)
    else:
        print(f"Success: Read: {num_lines}, insert: {row_count} Records (header skipped) into table: {table_name}")

    db.close_connection(conn)
    return row_count


def run(run_flag, run_id):
    print(f'=====> Running: {Path(__file__).name} for Job: {run_flag} and run_id: {run_id} <===== ')
    table_name, columns = get_raw_table_info(run_flag)
    row_count = store_csv(table_name,  columns, run_flag)
    update_run_stats(run_flag, run_id, table_name, len(columns), row_count)


if __name__ == "__main__":
    run_flag = "ny_school_math_score_result"
    run_id = 1
    run(run_flag, run_id)
    exit(0)


