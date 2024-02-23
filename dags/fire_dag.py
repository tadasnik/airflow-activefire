import datetime

import pendulum

from pathlib import Path
from dotenv import dotenv_values, find_dotenv


from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

config = dotenv_values(find_dotenv())
supabase_url: str | None = config["SUPABASE_URL"]
supabase_key: str | None = config["SUPABASE_KEY"]


@dag(
    schedule='@hourly',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test"],
)
def fire_data_etl():

    @task()
    def extract():

        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data pipeline
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

        from activefire.firedata import populate_db

        pc = populate_db.ProcSQL("VIIRS_NPP")
        pc.load_nrt()

    @task()
    def load_uk_detections(uk_transformed_dfr):

        from fireObsBackend.supabase_db import SupaDb
        sb = SupaDb(supabase_url, supabase_key)
        response = sb.upsert_uk_detections(uk_transformed_dfr)
        print(response)

    uk_dfr = extract_uk()
    uk_transformed_dfr = transform_uk(uk_dfr)
    
    extract() >> transform() >> load() >> uk_dfr >> uk_transformed_dfr >> load_uk_detections(uk_transformed_dfr)





fire_data_etl()

