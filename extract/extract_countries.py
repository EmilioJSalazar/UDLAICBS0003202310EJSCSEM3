from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def ext_countries():
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

        #Dictionary dfor values of countries_ext
        country_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[]
        }

        #Reading the CSV file
        country_csv=pd.read_csv("csvs/countries.csv")
        print(country_csv)

        #Processing the CSV file content
        if not country_csv.empty:
            for id,nam,reg,reg_id \
                in zip(country_csv['COUNTRY_ID'],country_csv['COUNTRY_NAME'],
                country_csv['COUNTRY_REGION'], country_csv['COUNTRY_REGION_ID']):
                country_dict["country_id"].append(id)
                country_dict["country_name"].append(nam)
                country_dict["country_region"].append(reg)
                country_dict["country_region_id"].append(reg_id)
        if country_dict['country_id']:
            ses_db_stg.connect().execute("TRUNCATE TABLE countries_ext")
            df_country_ext=pd.DataFrame(country_dict)
            df_country_ext.to_sql('countries_ext',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass