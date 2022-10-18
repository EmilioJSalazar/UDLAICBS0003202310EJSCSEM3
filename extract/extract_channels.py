from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def ext_channels():
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
        channel_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }

        #Reading the CSV file
        channel_csv=pd.read_csv("csvs/channels.csv")
        print(channel_csv)

        #Processing the CSV file content
        if not channel_csv.empty:
            for id,des,cla,cla_id \
                in zip(channel_csv['CHANNEL_ID'],channel_csv['CHANNEL_DESC'],
                channel_csv['CHANNEL_CLASS'], channel_csv['CHANNEL_CLASS_ID']):
                channel_dict["channel_id"].append(id)
                channel_dict["channel_desc"].append(des)
                channel_dict["channel_class"].append(cla)
                channel_dict["channel_class_id"].append(cla_id)
        if channel_dict['channel_id']:
            ses_db_stg.connect().execute("TRUNCATE TABLE channels_ext")
            df_channels_ext=pd.DataFrame(channel_dict)
            df_channels_ext.to_sql('channels_ext',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass