from pathlib import Path
import os

from Utils.log_utils import log_time
from Utils.sql_utils import *


def run(run_flag, run_id):
    print(f'=====> Running: {Path(__file__).name} for Job: {run_flag} and run_id: {run_id} <===== ')
    validate_enriched(run_flag, run_id)
    df = get_raw_school_math_score(run_flag)
    df = trim_all_columns(df)
    df = drop_dups(df)
    df = drop_bad_score(df)
    df = enrich_data(df, run_flag, run_id)
    store_enriched_df(df, run_flag, run_id)


if __name__ == "__main__":
    run_flag = "ny_school_math_score_result"
    run_id = 1
    run(run_flag, run_id)
    exit(0)


