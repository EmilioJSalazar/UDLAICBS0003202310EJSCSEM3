from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_sor, config_stg

def load_times(process):
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
        tim_dict = {
            "TIME_ID":[],
            "DAY_NAME":[],
            "DAY_NUMBER_IN_WEEK":[],
            "DAY_NUMBER_IN_MONTH":[],
            "CALENDAR_WEEK_NUMBER":[],
            "CALENDAR_MONTH_NUMBER":[],
            "CALENDAR_MONTH_DESC":[],
            "END_OF_CAL_MONTH":[],
            "CALENDAR_QUARTER_DESC":[],
            "CALENDAR_YEAR":[],
        }

        #Reading the sql query
        tim_tra=pd.read_sql(f"SELECT TIME_ID, DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH, CALENDAR_WEEK_NUMBER, CALENDAR_MONTH_NUMBER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR from TIMES_TRA where PROCESS_ID={process}", ses_db_stg)
        tim_sor=pd.read_sql(f"SELECT TIME_ID, DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH, CALENDAR_WEEK_NUMBER, CALENDAR_MONTH_NUMBER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR from TIMES", ses_db_sor)
        tim_sor.to_dict()
        #Processing the sql query content
        if not tim_tra.empty:
            for id,day_nam,day_num_wee,day_num_mon,cal_wee_num,cal_mon_num,cal_mon_des,end,cal_qua,cal_yea  \
                in zip(tim_tra['TIME_ID'],tim_tra['DAY_NAME'],
                tim_tra['DAY_NUMBER_IN_WEEK'], tim_tra['DAY_NUMBER_IN_MONTH'],
                tim_tra['CALENDAR_WEEK_NUMBER'],tim_tra['CALENDAR_MONTH_NUMBER'],
                tim_tra['CALENDAR_MONTH_DESC'], tim_tra['END_OF_CAL_MONTH'],
                tim_tra['CALENDAR_QUARTER_DESC'], tim_tra['CALENDAR_YEAR']):
                tim_dict["TIME_ID"].append(id)
                tim_dict["DAY_NAME"].append(day_nam)
                tim_dict["DAY_NUMBER_IN_WEEK"].append(day_num_wee)
                tim_dict["DAY_NUMBER_IN_MONTH"].append(day_num_mon)
                tim_dict["CALENDAR_WEEK_NUMBER"].append(cal_wee_num)
                tim_dict["CALENDAR_MONTH_NUMBER"].append(cal_mon_num)
                tim_dict["CALENDAR_MONTH_DESC"].append(cal_mon_des)
                tim_dict["END_OF_CAL_MONTH"].append(end)
                tim_dict["CALENDAR_QUARTER_DESC"].append(cal_qua)
                tim_dict["CALENDAR_YEAR"].append(cal_yea)

        if tim_dict['TIME_ID']:
            df_tim=pd.DataFrame(tim_dict)
            # df_tim.to_sql('times',ses_db_sor,if_exists='append',index=False)
            tim_merge = df_tim.merge(tim_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            tim_merge.to_sql('times', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass