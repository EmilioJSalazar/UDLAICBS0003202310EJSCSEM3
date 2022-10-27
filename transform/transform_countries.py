from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def tra_countries(process):
    try:
        configstg=config_stg()
        type = configstg['TYPE']
        host = configstg['HOST']
        port = configstg['PORT']
        user = configstg['USER']
        pwd = configstg['PWD']
        db = configstg['SCHEMA']

        con_db_stg = Db_Connection(type, host, port, user, pwd, db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to the b2b_dwh_staging database")

        df=pd.read_sql('SELECT USER()',ses_db_stg)
        print(df)

        #Dictionary dfor values of channels_ext
        cou_tra_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            "process_id":[]
        }

        #Reading the CSV file
        cha_ext=pd.read_sql("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID from COUNTRIES_EXT", ses_db_stg)

        #Processing the CSV file content
        if not cha_ext.empty:
            for id,nam,reg,reg_id  \
                in zip(cha_ext['COUNTRY_ID'],cha_ext['COUNTRY_NAME'],
                cha_ext['COUNTRY_REGION'], cha_ext['COUNTRY_REGION_ID']):
                cou_tra_dict["country_id"].append(id)
                cou_tra_dict["country_name"].append(nam)
                cou_tra_dict["country_region"].append(reg)
                cou_tra_dict["country_region_id"].append(reg_id)
                cou_tra_dict["process_id"].append(process)
        if cou_tra_dict['country_id']:
            df_cha_tra=pd.DataFrame(cou_tra_dict)
            df_cha_tra.to_sql('countries_tra',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass