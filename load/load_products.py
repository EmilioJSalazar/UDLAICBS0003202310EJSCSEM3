from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_sor, config_stg

def load_products(process):
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
        pro_dict = {
            "PROD_ID":[],
            "PROD_NAME":[],
            "PROD_DESC":[],
            "PROD_CATEGORY":[],
            "PROD_CATEGORY_ID":[],
            "PROD_CATEGORY_DESC":[],
            "PROD_WEIGHT_CLASS":[],
            "SUPPLIER_ID":[],
            "PROD_STATUS":[],
            "PROD_LIST_PRICE":[],
            "PROD_MIN_PRICE":[],
        }

        #Reading the sql query
        pro_tra=pd.read_sql(f"SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY, PROD_CATEGORY_ID, PROD_CATEGORY_DESC, PROD_WEIGHT_CLASS, SUPPLIER_ID, PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE from PRODUCTS_TRA where PROCESS_ID={process}", ses_db_stg)
        pro_sor=pd.read_sql(f"SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY, PROD_CATEGORY_ID, PROD_CATEGORY_DESC, PROD_WEIGHT_CLASS, SUPPLIER_ID, PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE from PRODUCTS", ses_db_sor)
        pro_sor.to_dict()
        #Processing the sql query content
        if not pro_tra.empty:
            for id,nam,des,cat,cat_id,cat_des,wei,sup,sta,lis,min  \
                in zip(pro_tra['PROD_ID'],pro_tra['PROD_NAME'],
                pro_tra['PROD_DESC'], pro_tra['PROD_CATEGORY'],
                pro_tra['PROD_CATEGORY_ID'], pro_tra['PROD_CATEGORY_DESC'],
                pro_tra['PROD_WEIGHT_CLASS'], pro_tra['SUPPLIER_ID'],
                pro_tra['PROD_STATUS'], pro_tra['PROD_LIST_PRICE'],
                pro_tra['PROD_MIN_PRICE']):
                pro_dict["PROD_ID"].append(id)
                pro_dict["PROD_NAME"].append(nam)
                pro_dict["PROD_DESC"].append(des)
                pro_dict["PROD_CATEGORY"].append(cat)
                pro_dict["PROD_CATEGORY_ID"].append(cat_id)
                pro_dict["PROD_CATEGORY_DESC"].append(cat_des)
                pro_dict["PROD_WEIGHT_CLASS"].append(wei)
                pro_dict["SUPPLIER_ID"].append(sup)
                pro_dict["PROD_STATUS"].append(sta)
                pro_dict["PROD_LIST_PRICE"].append(lis)
                pro_dict["PROD_MIN_PRICE"].append(min)


        if pro_dict['PROD_ID']:
            df_pro=pd.DataFrame(pro_dict)
            # df_pro.to_sql('products',ses_db_sor,if_exists='append',index=False)
            pro_merge = df_pro.merge(pro_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            pro_merge.to_sql('products', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass