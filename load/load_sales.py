from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_sor, config_stg

def load_sales(process):
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
        sal_dict = {
            "PROD_ID":[],
            "CUST_ID":[],
            "TIME_ID":[],
            "CHANNEL_ID":[],
            "PROMO_ID":[],
            "QUANTITY_SOLD":[],
            "AMOUNT_SOLD":[],
        }

        #Reading the sql query
        sal_tra=pd.read_sql(f"SELECT PROD_ID, CUST_ID, TIME_ID, CHANNEL_ID, PROMO_ID, QUANTITY_SOLD, AMOUNT_SOLD from SALES_TRA where PROCESS_ID={process}", ses_db_stg)
        sal_sor=pd.read_sql(f"SELECT PROD_ID, CUST_ID, TIME_ID, CHANNEL_ID, PROMO_ID, QUANTITY_SOLD, AMOUNT_SOLD from SALES", ses_db_sor)
        sal_sor.to_dict()

        pro_sor=pd.read_sql(f"SELECT ID, PROD_ID FROM PRODUCTS", ses_db_sor)
        cus_sor=pd.read_sql(f"SELECT ID, CUST_ID FROM CUSTOMERS", ses_db_sor)
        cha_sor=pd.read_sql(f"SELECT ID, CHANNEL_ID FROM CHANNELS", ses_db_sor)
        prom_sor=pd.read_sql(f"SELECT ID, PROMO_ID FROM PROMOTIONS", ses_db_sor)

        pro_dict=dict()
        if not pro_sor.empty:
            for id, pro_id \
                in zip(pro_sor['ID'], pro_sor['PROD_ID']):
                pro_dict[pro_id] = id

        cus_dict=dict()
        if not cus_sor.empty:
            for id, cus_id \
                in zip(cus_sor['ID'],cus_sor['CUST_ID']):
                cus_dict[cus_id] = id

        cha_dict=dict()
        if not cha_sor.empty:
            for id, ch_id \
                in zip(cha_sor['ID'],cha_sor['CHANNEL_ID']):
                cha_dict[ch_id] = id

        prom_dict=dict()
        if not prom_sor.empty:
            for id, prom_id \
                in zip(prom_sor['ID'],prom_sor['PROMO_ID']):
                prom_dict[prom_id] = id

        #Processing the sql query content
        if not sal_tra.empty:
            for pro,cus,tim,cha,prom,qua,amo  \
                in zip(sal_tra['PROD_ID'],sal_tra['CUST_ID'],
                sal_tra['TIME_ID'], sal_tra['CHANNEL_ID'],
                sal_tra['PROMO_ID'],sal_tra['QUANTITY_SOLD'],
                sal_tra['AMOUNT_SOLD']):
                sal_dict["PROD_ID"].append(pro_dict[pro])
                sal_dict["CUST_ID"].append(cus_dict[cus])
                sal_dict["TIME_ID"].append(tim)
                sal_dict["CHANNEL_ID"].append(cha_dict[cha])
                sal_dict["PROMO_ID"].append(prom_dict[prom])
                sal_dict["QUANTITY_SOLD"].append(qua)
                sal_dict["AMOUNT_SOLD"].append(amo)


        if sal_dict['PROD_ID']:
            df_sal=pd.DataFrame(sal_dict)
            df_sal.to_sql('sales',ses_db_sor,if_exists='append',index=False)
            # merge(table_name='channels', natural_key_cols=['channel_id'], dataframe= df_cha, db_context=ses_db_sor);
    except:
        traceback.print_exc()
    finally:
        pass