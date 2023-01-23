URL = "https://remotive.com/api/remote-jobs"

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import shutil
from datetime import datetime, timedelta

from airflow.decorators import dag, task


log = logging.getLogger(__name__)

requirements = ["requests", "psycopg2-binary"]

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
            now = cl.date_utils.get_today_string()
            fs = cl.filesystem.DataFilesystem(f"remotive")

            filename = f"{now}_remotive.json"
            json = fs.read_json(filename, json)

            if json is None:
                json = cl.remotive_api.GetJSON()
                fs.write_dict_as_json(filename, json)

            return json

        @task(multiple_outputs=True)
        def transform(order_data_dict: dict):
            """
            #### Transform task
            A simple Transform task which takes in the collection of order data and
            computes the total order value.
            """
            total_order_value = 0

            for value in order_data_dict.values():
                total_order_value += value

            return {"total_order_value": total_order_value}

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=requirements,
        )
        def load(total_order_value: float):
            """
            #### Load task
            A simple Load task which takes in the result of the Transform task and
            instead of saving it to end user review, just prints it out.
            """
            import sys
            sys.path.append("/home/paolo/airflow/dags")

            from custom_code.custom_lib import database
            with database.PostgresConnection() as connection:

                print(f"Total order value is: {total_order_value:.2f}")

        order_data = extract()
        order_summary = transform(order_data)
        load(order_summary["total_order_value"])


    tutorial_dag = consume_remotive()