from jproperties import Properties

def config_stg():
    config = Properties()
    with open('util\configuration.properties', 'rb') as stg_config_file:
        config.load(stg_config_file)
    stg_dict={
        "TYPE": config.get("TYPE").data,
        "HOST": config.get("DB_HOST").data,
        "PORT": config.get("DB_PORT").data,
        "USER": config.get("DB_USER").data,
        "PWD": config.get("DB_PWD").data,
        "SCHEMA": config.get("DB_SCHEMA").data,
    }
    return stg_dict