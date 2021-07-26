import errno
from pathlib import Path

from Utils.log_utils import log_time
from Utils.pipeline_utils import get_input_file_details
import os
import urllib.request

from Utils.sql_utils import insert_run_stats, validate_stats

path2data = os.getcwd()
output_dir = path2data + "\\data\\"


@log_time
def download_file(url, data_file):
    """
    Download csv file from server
    """
    try:
        os.makedirs(output_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    with urllib.request.urlopen(url) as testfile, open(data_file, 'w') as f:
        f.write(testfile.read().decode())

    # check if file exists
    if os.path.isfile(data_file):
        print(f"File {data_file} has been downoaded")
    else:
        print(f"File {data_file} has NOT been downoaded")
        exit(1)


def run(run_flag, run_id):
    print(f'=====> Running: {Path(__file__).name} for Job: {run_flag} and run_id: {run_id} <===== ')
    url, file_name = get_input_file_details(run_flag)
    data_file = output_dir + file_name
    validate_stats(run_flag, run_id)
    download_file(url, data_file)
    insert_run_stats(run_flag, run_id, file_name, output_dir)


if __name__ == "__main__":
    run_flag = "ny_school_math_score_result"
    run_id = 1
    run(run_flag, run_id)
    exit(0)


