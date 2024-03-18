from selectolax.parser import HTMLParser
import requests
import pandas as pd
import time

from database import MySQLConnector
      
        
def load_data(data, filename):
    """Save data into database
    Args:
        data (_type_): html content
        filename
    """
    table_data = pd.read_html(data)[0]
    df = pd.DataFrame(table_data)
    if len(df) != 0:
        # df.to_csv(filename, index = False)
        connector = MySQLConnector(
            host='localhost', 
            database='database',
            user='user',
            password='test'
            )
        connector.connect()
        connector.insert_dataframe(df,'amfi')
        connector.disconnect()

def extract_city(content):
    """_summary_

    Args:
        content (_type_): html content

    Yields:
        _type_: city names
    """
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
    
    
def main(main_url= "https://www.amfiindia.com/investor-corner/online-center/locate-mf-distributor.aspx"):
    print("AMFI FETCHER VERSION 1.0")
    print("\n")
    
    script_begin = time.time()

    session = requests.session()
    data = session.get(main_url)
    city_count = 0
    
    session.close()
    print("Total Time Taken by Script: {} Seconds for: {} cities".format(int(time.time() - script_begin),city_count))
