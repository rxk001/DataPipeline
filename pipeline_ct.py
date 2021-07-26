from Utils.pipeline_utils import run

pipeline = ["get_data_file", "store_data_file", "store_data_file_enriched", "store_data_file_summary"]

if __name__ == '__main__':
    run_flag = "ct_school_math_score_result"
    run_id = 1
    run(pipeline, run_flag, run_id)
