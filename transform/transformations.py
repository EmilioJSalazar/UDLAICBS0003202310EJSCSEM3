from datetime import datetime

def join_2_strings(string1, string2):
    return f"{string1} {string2}"

def obt_gender(gen):
    if gen == 'M':
        return 'MASCULINO'
    elif gen == 'F':
        return 'FEMENINO'
    else:
        return 'NO DEFINIDO'

def date_month(date_str):
    fecha =  datetime.strptime(date_str,'%d-%b-%y')
    return (fecha)    

def date_int(date_str):
    fecha = datetime.strptime(date_str,'%d-%b-%y')
    fecha_int = int(fecha.strftime('%d%m%y')) 
    return (fecha_int)    