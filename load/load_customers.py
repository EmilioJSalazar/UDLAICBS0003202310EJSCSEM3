from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_sor, config_stg

def load_customers(process):
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
        cus_dict = {
            "CUST_ID":[],
            "CUST_NOM_COM":[],
            "CUST_GEN":[],
            "CUST_YEAR_OF_BIRTH":[],
            "CUST_MARITAL_STATUS":[],
            "CUST_STREET_ADDRESS":[],
            "CUST_POSTAL_CODE":[],
            "CUST_CITY":[],
            "CUST_STATE_PROVINCE":[],
            "COUNTRY_ID":[],
            "CUST_MAIN_PHONE_NUMBER":[],
            "CUST_INCOME_LEVEL":[],
            "CUST_CREDIT_LIMIT":[],
            "CUST_EMAIL":[],
        }

        #Reading the sql query
        cus_tra=pd.read_sql(f"SELECT CUST_ID, CUST_NOM_COM, CUST_GEN, CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS, CUST_STREET_ADDRESS, CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE, COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL, CUST_CREDIT_LIMIT, CUST_EMAIL from CUSTOMERS_TRA where PROCESS_ID={process}", ses_db_stg)
        cus_sor=pd.read_sql(f"SELECT CUST_ID, CUST_NOM_COM, CUST_GEN, CUST_YEAR_OF_BIRTH, CUST_MARITAL_STATUS, CUST_STREET_ADDRESS, CUST_POSTAL_CODE, CUST_CITY, CUST_STATE_PROVINCE, COUNTRY_ID, CUST_MAIN_PHONE_NUMBER, CUST_INCOME_LEVEL, CUST_CREDIT_LIMIT, CUST_EMAIL from CUSTOMERS", ses_db_sor)
        cus_sor.to_dict()
        
        cou_sor=pd.read_sql(f"SELECT ID, COUNTRY_ID FROM COUNTRIES", ses_db_sor)
        cou_dict=dict()
        if not cou_sor.empty:
            for id, cou_id \
                in zip(cou_sor['ID'], cou_sor['COUNTRY_ID']):
                cou_dict[cou_id] = id

        #Processing the sql query content
        if not cus_tra.empty:
            for id,nom,gen,yea,mar,str,pos,cit,sta,cou,mai,inc,cre,ema  \
                in zip(cus_tra['CUST_ID'],cus_tra['CUST_NOM_COM'],
                cus_tra['CUST_GEN'],
                cus_tra['CUST_YEAR_OF_BIRTH'], cus_tra['CUST_MARITAL_STATUS'],
                cus_tra['CUST_STREET_ADDRESS'], cus_tra['CUST_POSTAL_CODE'],
                cus_tra['CUST_CITY'], cus_tra['CUST_STATE_PROVINCE'],
                cus_tra['COUNTRY_ID'], cus_tra['CUST_MAIN_PHONE_NUMBER'],
                cus_tra['CUST_INCOME_LEVEL'], cus_tra['CUST_CREDIT_LIMIT'],
                cus_tra['CUST_EMAIL']):
                cus_dict["CUST_ID"].append(id)
                cus_dict["CUST_NOM_COM"].append(nom)
                cus_dict["CUST_GEN"].append(gen)
                cus_dict["CUST_YEAR_OF_BIRTH"].append(yea)
                cus_dict["CUST_MARITAL_STATUS"].append(mar)
                cus_dict["CUST_STREET_ADDRESS"].append(str)
                cus_dict["CUST_POSTAL_CODE"].append(pos)
                cus_dict["CUST_CITY"].append(cit)
                cus_dict["CUST_STATE_PROVINCE"].append(sta)
                cus_dict["COUNTRY_ID"].append(cou_dict[cou])
                cus_dict["CUST_MAIN_PHONE_NUMBER"].append(mai)
                cus_dict["CUST_INCOME_LEVEL"].append(inc)
                cus_dict["CUST_CREDIT_LIMIT"].append(cre)
                cus_dict["CUST_EMAIL"].append(ema)


        if cus_dict['CUST_ID']:
            df_cus=pd.DataFrame(cus_dict)
            # df_cus.to_sql('customers',ses_db_sor,if_exists='append',index=False)
            cus_merge = df_cus.merge(cus_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            cus_merge.to_sql('customers', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass