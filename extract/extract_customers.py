from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def ext_customers():
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

        #Dictionary dfor values of customers_ext
        customer_dict = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_number":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[],
        }

        #Reading the CSV file
        customer_csv=pd.read_csv("csvs/customers.csv")
        print(customer_csv)

        #Processing the CSV file content
        if not customer_csv.empty:
            for id,fir,las,gen,yea,mar,str,pos,cit,sta,cou,mai,inc,cre,ema \
                in zip(customer_csv['CUST_ID'],customer_csv['CUST_FIRST_NAME'],
                customer_csv['CUST_LAST_NAME'], customer_csv['CUST_GENDER'],
                customer_csv['CUST_YEAR_OF_BIRTH'], customer_csv['CUST_MARITAL_STATUS'],
                customer_csv['CUST_STREET_ADDRESS'], customer_csv['CUST_POSTAL_CODE'],
                customer_csv['CUST_CITY'], customer_csv['CUST_STATE_PROVINCE'],
                customer_csv['COUNTRY_ID'], customer_csv['CUST_MAIN_PHONE_NUMBER'],
                customer_csv['CUST_INCOME_LEVEL'], customer_csv['CUST_CREDIT_LIMIT'],
                customer_csv['CUST_EMAIL'],):
                customer_dict["cust_id"].append(id)
                customer_dict["cust_first_name"].append(fir)
                customer_dict["cust_last_name"].append(las)
                customer_dict["cust_gender"].append(gen)
                customer_dict["cust_year_of_birth"].append(yea)
                customer_dict["cust_marital_status"].append(mar)
                customer_dict["cust_street_address"].append(str)
                customer_dict["cust_postal_code"].append(pos)
                customer_dict["cust_city"].append(cit)
                customer_dict["cust_state_province"].append(sta)
                customer_dict["country_id"].append(cou)
                customer_dict["cust_main_phone_number"].append(mai)
                customer_dict["cust_income_level"].append(inc)
                customer_dict["cust_credit_limit"].append(cre)
                customer_dict["cust_email"].append(ema)
        if customer_dict['cust_id']:
            ses_db_stg.connect().execute("TRUNCATE TABLE customers_ext")
            df_customers_ext=pd.DataFrame(customer_dict)
            df_customers_ext.to_sql('customers_ext',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass