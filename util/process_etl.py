from util.db_connection import Db_Connection
import traceback

from util.properties import config_stg

def process_etl():
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

        ses_db_stg.execute('INSERT INTO PROCESS_ETL values ()')
        check=ses_db_stg.execute('SELECT PROCESS_ID  FROM PROCESS_ETL ORDER BY PROCESS_ID DESC LIMIT 1').scalar()
        return check
    except:
        traceback.print_exc()
    finally:
        pass