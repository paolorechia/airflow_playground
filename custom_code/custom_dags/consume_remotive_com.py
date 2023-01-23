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
            import re
            import pandas as pd

            fp = cl.path_utils.get_today_remotive()
            df = pd.read_json(fp)

            sw_df = df[df["category"] == "Software Development"]
            sw_df.head()
            sw_df.candidate_required_location.unique()
            sw_df["location"] = sw_df.candidate_required_location.str.lower()

            weg = sw_df[
                (sw_df["location"].str.contains("europe"))
                | (sw_df["location"].str.contains("germany"))
                | (sw_df["location"].str.contains("worldwide"))
            ]

            currency_api = cl.currency_api.CachedCurrencyAPI()

            def salary_cleaner(s: str):
                out = s.lower()
                number_k = out.count("k")

                is_hourly = False
                is_monthly = False
                is_dollar = False
                is_euro = False

                if number_k >= 1:
                    out = out.replace("k", "000")

                if "hour" in out:
                    is_hourly = True
                if "month" in out:
                    is_monthly = True

                if "$" in out or "dollar" in out:
                    is_dollar = True

                if "€" in out or "euro" in out:
                    is_euro = True

                if is_hourly and is_monthly:
                    raise Exception(f"Is it hourly on monthly salary?? {s}") 

                if is_dollar and is_euro:
                    raise Exception(f"Is it in dollar or euro? {s}")

                numbers = re.findall("\d+", out)
                salary = -1
                if len(numbers) == 0:
                    return float(salary)
                elif len(numbers) == 1:
                    salary = float(numbers[0])
                # If it's a range, take average
                elif len(numbers) == 2:
                    salary = (float(numbers[0]) + float(numbers[1])) / 2
                else:
                    raise Exception(f"Unhandled case, 3 or more salary numbers? {s}")
                if is_dollar:
                    salary_in_euros = currency_api.convert_usd_to_euros(salary)
                # Else assume it's euros
                else:
                    salary_in_euros = salary
                
                if is_monthly:
                    annual_salary_in_euros = salary_in_euros * 12
                elif is_hourly:
                    # 8 hours / day, 20 days / month, 12 months / year
                    annual_salary_in_euros = salary_in_euros * 8 * 20 * 12
                else:
                    annual_salary_in_euros = salary_in_euros
                return annual_salary_in_euros

            weg["salary"] = weg["salary"].apply(salary_cleaner)

            return json

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=requirements,
        )
        def load(total_order_value: float):
            """
            #### Load task
            A simple Load task which takes the result and saves to Postgres.
            Not yet fully implemented.
            """
            import sys
            sys.path.append("/home/paolo/airflow/dags")

            from custom_code.custom_lib import database
            with database.PostgresConnection() as connection:
                pass

        today_json = extract()
        cleaned_json = transform(today_json)
        load(cleaned_json)


    tutorial_dag = consume_remotive()