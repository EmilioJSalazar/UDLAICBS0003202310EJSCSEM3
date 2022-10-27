from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_sor, config_stg

def load_countries(process):
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

        configsor=config_sor()
        type = configsor['TYPE']
        host = configsor['HOST']
        port = configsor['PORT']
        user = configsor['USER']
        pwd = configsor['PWD']
        db = configsor['SCHEMA_SOR']

        con_db_sor = Db_Connection(type, host, port, user, pwd, db)
        ses_db_sor = con_db_sor.start()
        if ses_db_sor == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_sor == -2:
            raise Exception("Error trying to connect to the b2b_dwh_sor database")

        #Dictionary dfor values of channels_ext
        cou_dict = {
            "COUNTRY_ID":[],
            "COUNTRY_NAME":[],
            "COUNTRY_REGION":[],
            "COUNTRY_REGION_ID":[],
        }

        #Reading the sql query
        cou_tra=pd.read_sql(f"SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID from COUNTRIES_TRA where PROCESS_ID={process}", ses_db_stg)
        cou_sor=pd.read_sql(f"SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM COUNTRIES", ses_db_sor)
        cou_sor.to_dict()
        #Processing the sql query content
        if not cou_tra.empty:
            for id,nam,reg,reg_id  \
                in zip(cou_tra['COUNTRY_ID'],cou_tra['COUNTRY_NAME'],
                cou_tra['COUNTRY_REGION'], cou_tra['COUNTRY_REGION_ID']):
                cou_dict["COUNTRY_ID"].append(id)
                cou_dict["COUNTRY_NAME"].append(nam)
                cou_dict["COUNTRY_REGION"].append(reg)
                cou_dict["COUNTRY_REGION_ID"].append(reg_id)


        if cou_dict['COUNTRY_ID']:
            df_cou=pd.DataFrame(cou_dict)
            # df_cou.to_sql('countries',ses_db_sor,if_exists='append',index=False)
            cou_merge = df_cou.merge(cou_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            cou_merge.to_sql('countries', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass