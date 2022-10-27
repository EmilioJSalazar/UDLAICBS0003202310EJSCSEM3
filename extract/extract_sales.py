from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def ext_sales():
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

        #Dictionary dfor values of sales_ext
        sale_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[]
        }

        #Reading the CSV file
        sale_csv=pd.read_csv("csvs/sales.csv")
        print(sale_csv)

        #Processing the CSV file content
        if not sale_csv.empty:
            for pro,cus,tim,cha,prom,qua,amo \
                in zip(sale_csv['PROD_ID'],sale_csv['CUST_ID'],
                sale_csv['TIME_ID'], sale_csv['CHANNEL_ID'],
                sale_csv['PROMO_ID'],sale_csv['QUANTITY_SOLD'],
                sale_csv['AMOUNT_SOLD']):
                sale_dict["prod_id"].append(pro)
                sale_dict["cust_id"].append(cus)
                sale_dict["time_id"].append(tim)
                sale_dict["channel_id"].append(cha)
                sale_dict["promo_id"].append(prom)
                sale_dict["quantity_sold"].append(qua)
                sale_dict["amount_sold"].append(amo)
        if sale_dict['prod_id']:
            ses_db_stg.connect().execute("TRUNCATE TABLE sales_ext")
            df_sales_ext=pd.DataFrame(sale_dict)
            df_sales_ext.to_sql('sales_ext',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass