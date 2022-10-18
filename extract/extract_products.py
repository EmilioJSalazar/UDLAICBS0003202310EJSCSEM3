from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def ext_products():
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

        #Dictionary dfor values of products_ext
        product_dict = {
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
            "prod_min_price":[]
        }

        #Reading the CSV file
        product_csv=pd.read_csv("csvs/products.csv")
        print(product_csv)

        #Processing the CSV file content
        if not product_csv.empty:
            for id,nam,des,cat,cat_id,cat_des,wei,sup,sta,lis,min \
                in zip(product_csv['PROD_ID'],product_csv['PROD_NAME'],
                product_csv['PROD_DESC'], product_csv['PROD_CATEGORY'],
                product_csv['PROD_CATEGORY_ID'], product_csv['PROD_CATEGORY_DESC'],
                product_csv['PROD_WEIGHT_CLASS'], product_csv['SUPPLIER_ID'],
                product_csv['PROD_STATUS'], product_csv['PROD_LIST_PRICE'],
                product_csv['PROD_MIN_PRICE']):
                product_dict["prod_id"].append(id)
                product_dict["prod_name"].append(nam)
                product_dict["prod_desc"].append(des)
                product_dict["prod_category"].append(cat)
                product_dict["prod_category_id"].append(cat_id)
                product_dict["prod_category_desc"].append(cat_des)
                product_dict["prod_weight_class"].append(wei)
                product_dict["supplier_id"].append(sup)
                product_dict["prod_status"].append(sta)
                product_dict["prod_list_price"].append(lis)
                product_dict["prod_min_price"].append(min)
        if product_dict['prod_id']:
            ses_db_stg.connect().execute("TRUNCATE TABLE products_ext")
            df_products_ext=pd.DataFrame(product_dict)
            df_products_ext.to_sql('products_ext',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass