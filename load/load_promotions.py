from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_sor, config_stg

def load_promotions(process):
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
        prom_dict = {
            "PROMO_ID":[],
            "PROMO_NAME":[],
            "PROMO_COST":[],
            "PROMO_BEGIN_DATE":[],
            "PROMO_END_DATE":[],
        }

        #Reading the sql query
        prom_tra=pd.read_sql(f"SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE from PROMOTIONS_TRA where PROCESS_ID={process}", ses_db_stg)
        prom_sor=pd.read_sql(f"SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE from PROMOTIONS", ses_db_sor)
        prom_sor.to_dict()
        #Processing the sql query content
        if not prom_tra.empty:
            for id,nam,cos,beg,end  \
                in zip(prom_tra['PROMO_ID'],prom_tra['PROMO_NAME'],
                prom_tra['PROMO_COST'], prom_tra['PROMO_BEGIN_DATE'],
                prom_tra['PROMO_END_DATE']):
                prom_dict["PROMO_ID"].append(id)
                prom_dict["PROMO_NAME"].append(nam)
                prom_dict["PROMO_COST"].append(cos)
                prom_dict["PROMO_BEGIN_DATE"].append(beg)
                prom_dict["PROMO_END_DATE"].append(end)


        if prom_dict['PROMO_ID']:
            df_prom=pd.DataFrame(prom_dict)
            # df_prom.to_sql('promotions',ses_db_sor,if_exists='append',index=False)
            prom_merge = df_prom.merge(prom_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            prom_merge.to_sql('promotions', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass