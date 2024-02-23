#

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

from __future__ import annotations


# [START tutorial]

# [START import_module]

import json


import pendulum


from pathlib import Path
from dotenv import dotenv_values, find_dotenv

from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed
from airflow.exceptions import AirflowFailException


config = dotenv_values(find_dotenv())
supabase_url: str | None = config["SUPABASE_URL"]
supabase_key: str | None = config["SUPABASE_KEY"]


if is_venv_installed():
    print('Is virtualenv')
else:
    print('Not virtualenv')


@dag(

    schedule='@hourly',
    # schedule=None,

    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),

    catchup=False,

    tags=["test"],

)

def fire_data_etl():

    """

    ### TaskFlow API Tutorial Documentation

    This is a simple data pipeline example which demonstrates the use of

    the TaskFlow API using three simple tasks for Extract, Transform, and Load.

    Documentation that goes along with the Airflow TaskFlow API tutorial is

    located

    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)

    """

    # [END instantiate_dag]


    # [START extract]

    @task()

    def extract():

        """

        #### Extract task

        A simple Extract task to get data ready for the rest of the data

        pipeline. In this case, getting data is simulated by reading from a

        hardcoded JSON string.

        """
        from activefire.firedata import populate_db

        pc = populate_db.ProcSQL("VIIRS_NPP")
        new_data = pc.get_nrt()
        if not new_data:
            raise AirflowFailException("No new data!")

    @task()
    def extract_uk():

        from activefire.firedata.database import DataBase
        from activefire.firedata.prepare_uk import ProcSQLUK
        from fireObsBackend.supabase_db import SupaDb

        sb = SupaDb(supabase_url, supabase_key)
        response_max_id = sb.client.table('detections').select('id').order('id', desc=True).limit(1).single().execute()
        response_active = sb.client.table('detections').select('*').eq('active', True).execute()

        pcu = ProcSQLUK("VIIRS_NPP")
        uk_dfr = pcu.get_uk_fire_detections(response_max_id.data['id'])
        if uk_dfr.empty:
            raise AirflowFailException("No new data for UK!")
        else:
            return uk_dfr

    @task()
    def transform():

        """

        #### Transform task

        A simple Transform task which takes in the collection of order data and

        computes the total order value.

        """

        from activefire.firedata import populate_db

        pc = populate_db.ProcSQL("VIIRS_NPP")
        pc.transform_nrt() 
        print("transform")

    @task()

    def transform_uk(uk_dfr):

        from activefire.firedata.prepare_uk import ProcSQLUK

        pcu = ProcSQLUK("VIIRS_NPP")
        uk_transformed_dfr = pcu.transform_uk_nrt(uk_dfr)
        if uk_transformed_dfr.empty:
            raise AirflowFailException("No vegetation fire detections in UK!")
        else:
            return uk_transformed_dfr
    @task()

    def load():

        """

        #### Load task

        A simple Load task which takes in the result of the Transform task and

        instead of saving it to end user review, just prints it out.

        """

        from activefire.firedata import populate_db

        pc = populate_db.ProcSQL("VIIRS_NPP")
        pc.load_nrt()

    # [END load]


    # [START main_flow]
    @task()
    def load_uk_detections(uk_transformed_dfr):

        from fireObsBackend.supabase_db import SupaDb
        sb = SupaDb(supabase_url, supabase_key)
        response = sb.upsert_uk_detections(uk_transformed_dfr)
        print(response)



    uk_dfr = extract_uk()
    uk_transformed_dfr = transform_uk(uk_dfr)
    
    extract() >> transform() >> load() >> uk_dfr >> uk_transformed_dfr >> load_uk_detections(uk_transformed_dfr)

    # [END main_flow]



# [START dag_invocation]

fire_data_etl()

# [END dag_invocation]


# [END tutorial]
