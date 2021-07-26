import Utils.sql_utils as sql

input_file_details = {"ny_school_math_score_result":
                          {"file_name": "ny_school_math_score_result.csv",
                           "download_url": "https://data.cityofnewyork.us/api/views/x4ai-kstz/rows.csv?accessType=DOWNLOAD"},
                      "ct_school_math_score_result":
                          {"file_name": "ct_school_math_score_result.csv",
                           "download_url": "https://data.cityofnewyork.us/api/views/x4ai-kstz/rows.csv?accessType=DOWNLOAD"}
                      }


def run(pipeline, run_flag, run_id):
    for i, p in enumerate(pipeline):
        module_to_import = p
        fcn_to_call = "run"
        mod = __import__(module_to_import)
        func = getattr(mod, fcn_to_call)
        func(run_flag, run_id)


def get_raw_table_name(job_name):
    if job_name in sql.raw_table_name:
        return sql.raw_table_name[job_name]
    raise NameError(f"=========> TABLES AND SCHEMAS for JOB: {job_name} are Not Setup <=========")


def get_raw_table_info(job_name):
    return get_raw_table_name(job_name), get_school_math_result_raw_table_columns()


def get_input_file_details(job_name):
    if job_name in input_file_details:
        details = input_file_details[job_name]
        file_name = details['file_name']
        download_url = details['download_url']
        return download_url, file_name
    raise NameError(f"=========> URL and FILE NAME for JOB: {job_name} are Not Setup <=========")


def get_school_math_result_raw_table_columns():
    return sql.SCHOOL_MATH_RESULT_RAW_TABLE_COLUMN
