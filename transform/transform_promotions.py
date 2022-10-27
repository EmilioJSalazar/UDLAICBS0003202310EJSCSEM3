from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def tra_promotions(process):
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
        prom_tra_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[],
            "process_id":[]
        }

        #Reading the CSV file
        prom_ext=pd.read_sql("SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE from PROMOTIONS_EXT", ses_db_stg)

        #Processing the CSV file content
        if not prom_ext.empty:
            for id,nam,cos,beg,end  \
                in zip(prom_ext['PROMO_ID'],prom_ext['PROMO_NAME'],
                prom_ext['PROMO_COST'], prom_ext['PROMO_BEGIN_DATE'],
                prom_ext['PROMO_END_DATE']):
                prom_tra_dict["promo_id"].append(id)
                prom_tra_dict["promo_name"].append(nam)
                prom_tra_dict["promo_cost"].append(cos)
                prom_tra_dict["promo_begin_date"].append(date_month(beg))
                prom_tra_dict["promo_end_date"].append(date_month(end))
                prom_tra_dict["process_id"].append(process)
        if prom_tra_dict['promo_id']:
            df_prom_tra=pd.DataFrame(prom_tra_dict)
            df_prom_tra.to_sql('promotions_tra',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass