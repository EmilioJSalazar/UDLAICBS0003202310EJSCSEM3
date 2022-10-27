
from extract.extract_channels import ext_channels
from extract.extract_countries import ext_countries
from extract.extract_customers import ext_customers
from extract.extract_products import ext_products
from extract.extract_promotions import ext_promotions
from extract.extract_sales import ext_sales
from extract.extract_times import ext_times
from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times
from transform.transform_countries import tra_countries
from transform.transform_customers import tra_customers
from transform.transform_products import tra_products
from transform.transform_promotions import tra_promotions
from transform.transform_sales import tra_sales
from transform.transform_times import tra_times
from util.process_etl import process_etl
import traceback

from transform.transform_channels import tra_channels
try:   

    # ext_channels()
    # ext_countries()
    # ext_customers()
    # ext_products()
    # ext_promotions()
    # ext_sales()
    # ext_times()
    
    # TRANSFORM
    process_num=process_etl()
    print("Número de proceso",process_num)
    tra_channels(process_num)
    tra_countries(process_num)
    tra_products(process_num)
    tra_promotions(process_num)
    tra_sales(process_num) 
    tra_times(process_num)
    tra_customers(process_num)

    # LOAD
    load_channels(process_num)
    load_countries(process_num)
    load_customers(process_num)
    load_products(process_num)
    load_promotions(process_num)
    load_times(process_num)
    load_sales(process_num)
    
except:
    traceback.print_exc()
finally:
    pass
