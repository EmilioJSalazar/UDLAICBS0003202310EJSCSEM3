from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_sor, config_stg

def load_channels(process):
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
        cha_dict = {
            "CHANNEL_ID":[],
            "CHANNEL_DESC":[],
            "CHANNEL_CLASS":[],
            "CHANNEL_CLASS_ID":[],
        }

        #Reading the sql query
        cha_tra=pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID from CHANNELS_TRA where PROCESS_ID={process}", ses_db_stg)
        cha_sor=pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM CHANNELS", ses_db_sor)
        cha_sor.to_dict()
        #Processing the sql query content
        if not cha_tra.empty:
            for id,des,cla,cla_id  \
                in zip(cha_tra['CHANNEL_ID'],cha_tra['CHANNEL_DESC'],
                cha_tra['CHANNEL_CLASS'], cha_tra['CHANNEL_CLASS_ID']):
                cha_dict["CHANNEL_ID"].append(id)
                cha_dict["CHANNEL_DESC"].append(des)
                cha_dict["CHANNEL_CLASS"].append(cla)
                cha_dict["CHANNEL_CLASS_ID"].append(cla_id)


        if cha_dict['CHANNEL_ID']:
            df_cha=pd.DataFrame(cha_dict)
            # df_cha.to_sql('channels',ses_db_sor,if_exists='append',index=False)
            cha_merge = df_cha.merge(cha_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            cha_merge.to_sql('channels', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass