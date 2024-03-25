from selectolax.parser import HTMLParser
import requests
import pandas as pd
import time
from datetime import datetime

from src.database import MySQLConnector

host="194.163.128.158"
user="amfi_user"
database="amfi_data"
password="BxH#X=eG[6r4s37h"
port="3066"
        
def load_data(data):
    """Save data into database
    Args:
        data (_type_): html content
        filename
    """
    print("loading data")
    table_data = pd.read_html(data)[0]
    df = pd.DataFrame(table_data)
    df.columns = ['sr', 'arn', 'holder_name', 'address', 'pin','email', 'city', 'telephone_r', 'telephone_o','arn_valid_till', 'arn_valid_from', 'kyd_compliant', 'EUIN']
    df.drop('sr', axis=1, inplace=True)
    df['IngestionTimeStamp'] = str(datetime.now())
    df['arn_valid_from'] = pd.to_datetime(df["arn_valid_from"])
    df['arn_valid_till'] = pd.to_datetime(df["arn_valid_till"])
    if len(df) != 0:

        connector = MySQLConnector(
            host=host, 
            database=database,
            user=user,
            password=password
            )
        connector.connect()
        connector.insert_or_update_dataframe(df,'amfi', 'arn')
        connector.disconnect()

def extract_city(content):
    """_summary_

    Args:
        content (_type_): html content

    Yields:
        _type_: city names
    """
    print("Extracting city names")
    html = HTMLParser(content.text)
    options = html.css("select#ddlCity > option")
    for option in options[2:]:
        city = option.attributes['value']
        yield city

def amfi_post_request(city_name):
    req_url = 'https://www.amfiindia.com/modules/NearestFinancialAdvisorsDetails'
    post_data = {"nfaType":"All","nfaARN":"","nfaARNName":"","nfaAddress":"","nfaCity":city_name,"nfaPin":""}
    req_post = requests.post(req_url, post_data)
    
    if req_post.status_code == 200:
        data = req_post.content
        req_post.close()
        return data
    
    else:
        req_post.close()
        return None
    
    
def amfi_scraper_main(main_url= "https://www.amfiindia.com/investor-corner/online-center/locate-mf-distributor.aspx"):
    print("AMFI FETCHER VERSION 1.0")
    print("\n")
    
    script_begin = time.time()

    data = requests.get(main_url)
    city_count = 0
    for city_name in extract_city(data):
        data = amfi_post_request(city_name)
        load_data(data)
