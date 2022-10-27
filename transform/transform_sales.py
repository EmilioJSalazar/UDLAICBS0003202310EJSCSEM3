from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def tra_sales(process):
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

        #Dictionary dfor values of channels_ext
        sal_tra_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
            "process_id":[]
        }

        #Reading the CSV file
        sal_ext=pd.read_sql("SELECT PROD_ID, CUST_ID, TIME_ID, CHANNEL_ID, PROMO_ID, QUANTITY_SOLD, AMOUNT_SOLD from SALES_EXT", ses_db_stg)

        #Processing the CSV file content
        if not sal_ext.empty:
            for pro,cus,tim,cha,prom,qua,amo  \
                in zip(sal_ext['PROD_ID'],sal_ext['CUST_ID'],
                sal_ext['TIME_ID'], sal_ext['CHANNEL_ID'],
                sal_ext['PROMO_ID'],sal_ext['QUANTITY_SOLD'],
                sal_ext['AMOUNT_SOLD']):
                sal_tra_dict["prod_id"].append(pro)
                sal_tra_dict["cust_id"].append(cus)
                sal_tra_dict["time_id"].append(date_int(tim))
                sal_tra_dict["channel_id"].append(cha)
                sal_tra_dict["promo_id"].append(prom)
                sal_tra_dict["quantity_sold"].append(qua)
                sal_tra_dict["amount_sold"].append(amo)
                sal_tra_dict["process_id"].append(process)
        if sal_tra_dict['prod_id']:
            df_sal_tra=pd.DataFrame(sal_tra_dict)
            df_sal_tra.to_sql('sales_tra',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass