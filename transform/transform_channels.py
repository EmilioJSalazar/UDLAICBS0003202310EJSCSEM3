from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def tra_channels(process):
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
        cha_tra_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[],
            "process_id":[]
        }

        #Reading the CSV file
        cha_ext=pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID from CHANNELS_EXT", ses_db_stg)

        #Processing the CSV file content
        if not cha_ext.empty:
            for id,des,cla,cla_id  \
                in zip(cha_ext['CHANNEL_ID'],cha_ext['CHANNEL_DESC'],
                cha_ext['CHANNEL_CLASS'], cha_ext['CHANNEL_CLASS_ID']):
                cha_tra_dict["channel_id"].append(id)
                cha_tra_dict["channel_desc"].append(des)
                cha_tra_dict["channel_class"].append(cla)
                cha_tra_dict["channel_class_id"].append(cla_id)
                cha_tra_dict["process_id"].append(process)
        if cha_tra_dict['channel_id']:
            df_cha_tra=pd.DataFrame(cha_tra_dict)
            df_cha_tra.to_sql('channels_tra',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass