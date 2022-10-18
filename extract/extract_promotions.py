from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def ext_promotions():
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

        #Dictionary dfor values of promotions_ext
        promotion_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }

        #Reading the CSV file
        promotion_csv=pd.read_csv("csvs/promotions.csv")
        print(promotion_csv)

        #Processing the CSV file content
        if not promotion_csv.empty:
            for id,nam,cos,beg,end \
                in zip(promotion_csv['PROMO_ID'],promotion_csv['PROMO_NAME'],
                promotion_csv['PROMO_COST'], promotion_csv['PROMO_BEGIN_DATE'],
                promotion_csv['PROMO_END_DATE']):
                promotion_dict["promo_id"].append(id)
                promotion_dict["promo_name"].append(nam)
                promotion_dict["promo_cost"].append(cos)
                promotion_dict["promo_begin_date"].append(beg)
                promotion_dict["promo_end_date"].append(end)
        if promotion_dict['promo_id']:
            ses_db_stg.connect().execute("TRUNCATE TABLE promotions_ext")
            df_promotions_ext=pd.DataFrame(promotion_dict)
            df_promotions_ext.to_sql('promotions_ext',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass