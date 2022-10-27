from transform.transformations import *
from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def tra_times(process):
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
        tim_tra_dict = {
            "time_id":[],
            "day_name":[],
            "day_number_in_week":[],
            "day_number_in_month":[],
            "calendar_week_number":[],
            "calendar_month_number":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_quarter_desc":[],
            "calendar_year":[],
            "process_id":[]
        }

        #Reading the CSV file
        tim_ext=pd.read_sql("SELECT TIME_ID, DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH, CALENDAR_WEEK_NUMBER, CALENDAR_MONTH_NUMBER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR from TIMES_EXT", ses_db_stg)

        #Processing the CSV file content
        if not tim_ext.empty:
            for id,day_nam,day_num_wee,day_num_mon,cal_wee_num,cal_mon_num,cal_mon_des,end,cal_qua,cal_yea  \
                in zip(tim_ext['TIME_ID'],tim_ext['DAY_NAME'],
                tim_ext['DAY_NUMBER_IN_WEEK'], tim_ext['DAY_NUMBER_IN_MONTH'],
                tim_ext['CALENDAR_WEEK_NUMBER'],tim_ext['CALENDAR_MONTH_NUMBER'],
                tim_ext['CALENDAR_MONTH_DESC'], tim_ext['END_OF_CAL_MONTH'],
                tim_ext['CALENDAR_QUARTER_DESC'], tim_ext['CALENDAR_YEAR']):
                tim_tra_dict["time_id"].append(date_int(id))
                tim_tra_dict["day_name"].append(day_nam)
                tim_tra_dict["day_number_in_week"].append(day_num_wee)
                tim_tra_dict["day_number_in_month"].append(day_num_mon)
                tim_tra_dict["calendar_week_number"].append(cal_wee_num)
                tim_tra_dict["calendar_month_number"].append(cal_mon_num)
                tim_tra_dict["calendar_month_desc"].append(cal_mon_des)
                tim_tra_dict["end_of_cal_month"].append(date_month(end))
                tim_tra_dict["calendar_quarter_desc"].append(cal_qua)
                tim_tra_dict["calendar_year"].append(cal_yea)
                tim_tra_dict["process_id"].append(process)
        if tim_tra_dict['time_id']:
            df_tim_tra=pd.DataFrame(tim_tra_dict)
            df_tim_tra.to_sql('times_tra',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass