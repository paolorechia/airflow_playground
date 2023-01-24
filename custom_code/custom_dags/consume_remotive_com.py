import logging
import shutil
from datetime import datetime, timedelta

from airflow.decorators import dag, task

log = logging.getLogger(__name__)


requirements = ["requests", "psycopg2-binary", "pandas"]

if not shutil.which("virtualenv"):
    log.warning("The tutorial_taskflow_api_virtualenv example DAG requires virtualenv, please install it.")
else:

    @dag(schedule=timedelta(days=1), start_date=datetime(2021, 1, 1), catchup=False, tags=["remotive", "software_jobs"])
    def consume_remotive():

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=requirements,
        )
        def extract():
            """
            #### Extract task
            A simple Extract task to get data ready for the rest of the data
            pipeline. In this case, getting data is simulated by reading from a
            hardcoded JSON string.
            """ 
            import sys
            sys.path.append("/home/paolo/airflow/dags")

            from custom_code import custom_lib as cl
            fs = cl.filesystem.DataFilesystem(f"remotive")

            filename = cl.path_utils.get_today_remotive()
            json = fs.read_json(filename)

            if json is None:
                json = cl.remotive_api.get_json()
                fs.write_dict_as_json(filename, json)

            return json


        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=requirements,
        )
        def transform(json):
            """Cleans up JSON.   
            
            To be done
            """
            import sys
            sys.path.append("/home/paolo/airflow/dags")

            from custom_code import custom_lib as cl
            import pandas as pd

            input_fp = cl.path_utils.get_today_remotive()
            output_fp = cl.path_utils.get_today_remotive("remotive-cleaned")

            df = pd.read_json(input_fp)

            # We don't care about other categories :)
            sw_df = df[df["category"] == "Software Development"]

            cleaner = cl.salary.clean_salary_factory()
            sw_df["salary"] = sw_df["salary"].apply(cleaner)

            sw_df.to_json(output_fp)

            return {"df_path": output_fp}

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=requirements,
        )
        def load(transform):
            """
            #### Load task
            A simple Load task which takes the result and saves to Postgres.
            Not yet fully implemented.
            """
            import sys
            sys.path.append("/home/paolo/airflow/dags")

            import pandas as pd

            from custom_code.custom_lib import database

            df_to_save = pd.DataFrame()

            cleaned_df = pd.read_json(transform["df_path"])            
            # df["location"]
            with database.PostgresConnection() as connection:
                pass

        today_json = extract()
        cleaned_json = transform(today_json)
        load(cleaned_json)


    tutorial_dag = consume_remotive()