# Compaign Pipeline

This repo contains the code for creating a data pipeline to calculate metrics for a compaign:

* `pipeline.py` -- Driver program. 
* `get_data_file.py` -- Downloads csv file. Inserts stats into load_statistics table in SQLite database.
* `store_data_file.py` -- Stores downloaded file into ny_school_math_score_result_raw/ct_school_math_score_result_raw (depend on a job) 
  table in SQLite database and updates load_statistics table.
* `store_data_file_enriched.py` -- Reads ny_school_math_score_result_raw/ct_school_math_score_result_raw,
  cleans data and adds state, run_id and update_dt and populate school_math_score_result_enriched table.
* `store_data_file_summary.py` -- Reads school_math_score_result_enriched, aggregates data and populate school_math_score_result_summary tabel.



# Installation

To get this repo running:

* Install Python 3
* Unzip attached file
* Get into the folder with: `cd DataPipeline`
* Create a virtual environment with: `python -m venv ./venv`
* Install the requirements with `pip install -r requirements.txt`


# Usage

* Execute pipeline with:
  python -m pipeline
  or 
  python -m pipeline_ct
  or 
  python -m pipeline_ny

To run different pipeline just create new pipeline_<>.py

To run part of the pipeline just modify pipeline list in pipeline.py and specify args: run_flag and run_id.
These parameters used to validate runs to avoid duplicate submission and allows ro run different input files.