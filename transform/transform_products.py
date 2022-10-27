from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def tra_products(process):
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
        pro_tra_dict = {
            "prod_id":[],
            "prod_name":[],
            "prod_desc":[],
            "prod_category":[],
            "prod_category_id":[],
            "prod_category_desc":[],
            "prod_weight_class":[],
            "supplier_id":[],
            "prod_status":[],
            "prod_list_price":[],
            "prod_min_price":[],
            "process_id":[]
        }

        #Reading the CSV file
        pro_ext=pd.read_sql("SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY, PROD_CATEGORY_ID, PROD_CATEGORY_DESC, PROD_WEIGHT_CLASS, SUPPLIER_ID, PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE from PRODUCTS_EXT", ses_db_stg)

        #Processing the CSV file content
        if not pro_ext.empty:
            for id,nam,des,cat,cat_id,cat_des,wei,sup,sta,lis,min  \
                in zip(pro_ext['PROD_ID'],pro_ext['PROD_NAME'],
                pro_ext['PROD_DESC'], pro_ext['PROD_CATEGORY'],
                pro_ext['PROD_CATEGORY_ID'], pro_ext['PROD_CATEGORY_DESC'],
                pro_ext['PROD_WEIGHT_CLASS'], pro_ext['SUPPLIER_ID'],
                pro_ext['PROD_STATUS'], pro_ext['PROD_LIST_PRICE'],
                pro_ext['PROD_MIN_PRICE']):
                pro_tra_dict["prod_id"].append(id)
                pro_tra_dict["prod_name"].append(nam)
                pro_tra_dict["prod_desc"].append(des)
                pro_tra_dict["prod_category"].append(cat)
                pro_tra_dict["prod_category_id"].append(cat_id)
                pro_tra_dict["prod_category_desc"].append(cat_des)
                pro_tra_dict["prod_weight_class"].append(wei)
                pro_tra_dict["supplier_id"].append(sup)
                pro_tra_dict["prod_status"].append(sta)
                pro_tra_dict["prod_list_price"].append(lis)
                pro_tra_dict["prod_min_price"].append(min)
                pro_tra_dict["process_id"].append(process)
        if pro_tra_dict['prod_id']:
            df_pro_tra=pd.DataFrame(pro_tra_dict)
            df_pro_tra.to_sql('products_tra',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass