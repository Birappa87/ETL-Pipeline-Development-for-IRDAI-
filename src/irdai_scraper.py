from playwright.sync_api import sync_playwright
import time
from selectolax.parser import HTMLParser
import pandas as pd
from datetime import datetime
import xmltodict
import requests
from collections import defaultdict

from src.database import MySQLConnector

columns = ['bntagentid', 'agentname', 'licenceno', 'irdaurn', 'agentid', 'insurancetype', 'insurer', 'dpid', 'state',   'district', 'pincode', 'validfrom', 'validto', 'absorbedagent', 'phoneno', 'mobile_no']

def get_states():
    state_info = {}
    url = 'https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetState'
    headers = {"Content-Type": "application/xml; charset=utf-8"}
    data = "{}"
    response = requests.post(url, headers=headers, data=data)
    data = xmltodict.parse(response.text)
    table_data = data['NewDataSet']['Table']
    for data in table_data:
        state_info[data['tntStateID']] = data['varStateName']
    return state_info

def get_district():
    district_info = defaultdict(list)
    state_info = get_states()
    url = 'https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetDistrict'
    headers = {"Content-Type": "application/xml; charset=utf-8"}

    for state_id in state_info.keys():
        params = {"StateID": state_id}
        try:
            response = requests.get(url, params=params)
        except:
            pass
        data = xmltodict.parse(response.text)
        table_data = data['NewDataSet']['Table']
        for data in table_data:
            try:
                district_info[state_id].append(data['sntDistrictID'])
            except:
                pass
    return district_info

def get_insurer_type():
    insurer_type_info = {}
    url = "https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetInsurerType"
    headers = {"Content-Type": "application/xml; charset=utf-8"}
    data = "{}"
    response = requests.post(url, headers=headers, data=data)
    data = xmltodict.parse(response.text)
    table_data = data['NewDataSet']['Table']
    for data in table_data:
        insurer_type_info[data['BintParamConstantValue']] = data['VcParamValueDisplay']
    return insurer_type_info

def get_insurer():
    insurer_info = {}
    url = "https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetInsurer"
    headers = {"Content-Type": "application/xml; charset=utf-8"}
    for insurance_type_id, insurance_type_name in get_insurer_type().items():
        params = {"InsuranceType" : insurance_type_id}
        response = requests.get(url, headers=headers, params=params)
        data = xmltodict.parse(response.text)
        table_data = data['NewDataSet']['Table']
        insurer_info[insurance_type_id] = [(insurer_data['intTblMstInsurerUserID'], insurer_data['varInsurerID']) for insurer_data in table_data]
    return insurer_info

def extract_data(page_content):
    tree = HTMLParser(page_content)
    table = tree.css_first('#fgAgentLocator')
    if table:
        rows = table.css('tr')
        data = []
        for row in rows:
            cells = row.css('td')
            row_data = [cell.text(strip=True) for cell in cells]
            data.append(row_data)
        
        # Construct DataFrame
        df = pd.DataFrame(data, columns=columns)
        df['ingestiontimestamp'] = str(datetime.now())
        df['validfrom'] = pd.to_datetime(df["validfrom"])
        df['validto'] = pd.to_datetime(df["validto"])
        host="194.163.128.158"
        user="amfi_user"
        database="amfi_data"
        password="BxH#X=eG[6r4s37h"
        port="3066"
        connector = MySQLConnector(
            host=host, 
            database=database,
            user=user,
            password=password
            )
        connector.connect()
        connector.insert_or_update_dataframe(df, 'irda', 'irdaurn')
        connector.disconnect()

def irdai_scraper_main():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=False,
            args=["--start-maximized"]
        )

        context = browser.new_context()

        page = context.new_page()
        url = "https://agencyportal.irdai.gov.in/PublicAccess/AgentLocator.aspx"
        page.goto(url)
        page.evaluate('(document.getElementById("ddlInsurer")).removeAttribute("disabled")')

        for insurance_type_id, insurer_list in get_insurer().items():
            for insurer_id in insurer_list:
                for state_id, district_list in get_district().items():
                    for district_id in district_list:
                        print(insurance_type_id, state_id, district_id)
                        page.select_option("#ddlInsuranceType", insurance_type_id)
                        page.select_option("#ddlInsurer", insurer_id)
                        page.select_option("#ddlState", state_id)
                        page.select_option("#ddlDistrict", district_id)
                        page.click('#btnLocate')
                        try:
                            # element = page.wait_for_selector("#fgAgentLocator", state="visible")
                            page_content = page.content()
                            extract_data(page_content)
                        except:
                            print("No record found")

        browser.close()
