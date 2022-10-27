from util.db_connection import Db_Connection
import pandas as pd
import traceback

from util.properties import config_stg

def ext_times():
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

        #Dictionary dfor values of times_ext
        time_dict = {
            "time_id":[],
            "day_name":[],
            "day_number_in_week":[],
            "day_number_in_month":[],
            "calendar_week_number":[],
            "calendar_month_number":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
        }

        #Reading the CSV file
        time_csv=pd.read_csv("csvs/times.csv")
        print(time_csv)

        #Processing the CSV file content
        if not time_csv.empty:
            for id,day_nam,day_num_wee,day_num_mon,cal_wee_num,cal_mon_num,cal_mon_des,end,cal_qua,cal_yea \
                in zip(time_csv['TIME_ID'],time_csv['DAY_NAME'],
                time_csv['DAY_NUMBER_IN_WEEK'], time_csv['DAY_NUMBER_IN_MONTH'],
                time_csv['CALENDAR_WEEK_NUMBER'],time_csv['CALENDAR_MONTH_NUMBER'],
                time_csv['CALENDAR_MONTH_DESC'], time_csv['END_OF_CAL_MONTH'],
                time_csv['CALENDAR_QUARTER_DESC'], time_csv['CALENDAR_YEAR']):
                time_dict["time_id"].append(id)
                time_dict["day_name"].append(day_nam)
                time_dict["day_number_in_week"].append(day_num_wee)
                time_dict["day_number_in_month"].append(day_num_mon)
                time_dict["calendar_week_number"].append(cal_wee_num)
                time_dict["calendar_month_number"].append(cal_mon_num)
                time_dict["calendar_month_desc"].append(cal_mon_des)
                time_dict["end_of_cal_month"].append(end)
                time_dict["calendar_quarter_desc"].append(cal_qua)
                time_dict["calendar_year"].append(cal_yea)
        if time_dict['time_id']:
            ses_db_stg.connect().execute("TRUNCATE TABLE times_ext")
            df_times_ext=pd.DataFrame(time_dict)
            df_times_ext.to_sql('times_ext',ses_db_stg,if_exists='append',index=False)
    except:
        traceback.print_exc()
    finally:
        pass