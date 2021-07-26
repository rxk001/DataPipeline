from pathlib import Path
import os

from Utils.sql_utils import *


def run(run_flag, run_id):
    print(f'=====> Running: {Path(__file__).name} for Job: {run_flag} and run_id: {run_id} <===== ')
    validate_summary(run_flag, run_id)
    df = get_math_score_enriched(run_id)
    df = get_math_score_summary(df)
    add_state_run_id_dt_data(df, run_flag, run_id)
    store_summary_df(df, run_flag, run_id)


if __name__ == "__main__":
    run_flag = "ny_school_math_score_result"
    run_id = 1
    run(run_flag, run_id)
    exit(0)


