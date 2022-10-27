from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def tra_customers(process):
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
        cus_tra_dict = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
            "cust_nom_com":[],
            "cust_gender":[],
            "cust_gen":[],
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
            "process_id":[]
        }

        #Reading the CSV file
        cus_ext=pd.read_sql("SELECT CUST_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CUST_GENDER, CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS, CUST_STREET_ADDRESS, CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE, COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL, CUST_CREDIT_LIMIT, CUST_EMAIL from CUSTOMERS_EXT", ses_db_stg)

        #Processing the CSV file content
        if not cus_ext.empty:
            for id,fir,las,gen,yea,mar,str,pos,cit,sta,cou,mai,inc,cre,ema  \
                in zip(cus_ext['CUST_ID'],cus_ext['CUST_FIRST_NAME'],
                cus_ext['CUST_LAST_NAME'], cus_ext['CUST_GENDER'],
                cus_ext['CUST_YEAR_OF_BIRTH'], cus_ext['CUST_MARITAL_STATUS'],
                cus_ext['CUST_STREET_ADDRESS'], cus_ext['CUST_POSTAL_CODE'],
                cus_ext['CUST_CITY'], cus_ext['CUST_STATE_PROVINCE'],
                cus_ext['COUNTRY_ID'], cus_ext['CUST_MAIN_PHONE_NUMBER'],
                cus_ext['CUST_INCOME_LEVEL'], cus_ext['CUST_CREDIT_LIMIT'],
                cus_ext['CUST_EMAIL']):
                cus_tra_dict["cust_id"].append(id)
                cus_tra_dict["cust_first_name"].append(fir)
                cus_tra_dict["cust_last_name"].append(las)
                cus_tra_dict["cust_nom_com"].append(join_2_strings(fir,las))
                cus_tra_dict["cust_gender"].append(gen)
                cus_tra_dict["cust_gen"].append(obt_gender(gen))
                cus_tra_dict["cust_year_of_birth"].append(yea)
                cus_tra_dict["cust_marital_status"].append(mar)
                cus_tra_dict["cust_street_address"].append(str)
                cus_tra_dict["cust_postal_code"].append(pos)
                cus_tra_dict["cust_city"].append(cit)
                cus_tra_dict["cust_state_province"].append(sta)
                cus_tra_dict["country_id"].append(cou)
                cus_tra_dict["cust_main_phone_number"].append(mai)
                cus_tra_dict["cust_income_level"].append(inc)
                cus_tra_dict["cust_credit_limit"].append(cre)
                cus_tra_dict["cust_email"].append(ema)
                cus_tra_dict["process_id"].append(process)
        if cus_tra_dict['cust_id']:
            df_cha_tra=pd.DataFrame(cus_tra_dict)
            df_cha_tra.to_sql('customers_tra',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass