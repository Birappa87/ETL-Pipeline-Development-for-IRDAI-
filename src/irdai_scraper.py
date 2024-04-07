import requests
from xmltodict import parse
from datetime import datetime
import pandas as pd
import xmltodict
from collections import defaultdict
from playwright.sync_api import sync_playwright
import logging as log
import time
from selectolax.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor

from src.database import load_dataframe_to_mysql

log.basicConfig(
    filename="logs/irdai_scraper.log",
    level=log.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d-%b-%Y %H:%M:%S",
)

columns = [
    "bntagentid",
    "agentname",
    "licenceno",
    "irdaurn",
    "agentid",
    "insurancetype",
    "insurer",
    "dpid",
    "state",
    "district",
    "pincode",
    "validfrom",
    "validto",
    "absorbedagent",
    "phoneno",
    "mobile_no",
    "ingestiontimestamp",
]

irdai_data = []


def get_states():
    """Fetches state information from IRDAI's web service."""
    url = (
        "https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetState"
    )
    headers = {"Content-Type": "application/xml; charset=utf-8"}
    data = "{}"
    response = requests.post(url, headers=headers, data=data)
    data = parse(response.text)
    table_data = data["NewDataSet"]["Table"]
    state_info = {record["tntStateID"]: record["varStateName"] for record in table_data}
    return state_info


def get_district(state_id):
    district_info = defaultdict(list)
    state_info = get_states()
    url = "https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetDistrict"
    headers = {"Content-Type": "application/xml; charset=utf-8"}

    for state_id in state_info.keys():
        params = {"StateID": state_id}
        try:
            response = requests.get(url, params=params)
        except:
            pass
        data = xmltodict.parse(response.text)
        table_data = data["NewDataSet"]["Table"]
        for data in table_data:
            try:
                district_info[state_id].append(data["sntDistrictID"])
            except:
                pass
    return district_info


def get_insurer_type():
    """Fetches insurer type information."""
    url = "https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetInsurerType"
    headers = {"Content-Type": "application/xml; charset=utf-8"}
    data = "{}"
    response = requests.post(url, headers=headers, data=data)
    data = parse(response.text)
    table_data = data["NewDataSet"]["Table"]
    return {
        record["BintParamConstantValue"]: record["VcParamValueDisplay"]
        for record in table_data
    }


def get_insurer():
    insurer_info = {}
    url = "https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetInsurer"
    headers = {"Content-Type": "application/xml; charset=utf-8"}

    for insurance_type_id, insurance_type_name in get_insurer_type().items():
        params = {"InsuranceType": insurance_type_id}
        response = requests.get(url, headers=headers, params=params)
        data = xmltodict.parse(response.text)
        table_data = data["NewDataSet"]["Table"]
        insurer_info[insurance_type_id] = [
            (insurer_data["intTblMstInsurerUserID"], insurer_data["varInsurerID"])
            for insurer_data in table_data
        ]
    return insurer_info


def extract_data(page_content):
    tree = HTMLParser(page_content)
    table = tree.css_first("#fgAgentLocator")
    if table:
        rows = table.css("tr")
        for row in rows:
            cells = row.css("td")
            row_data = [cell.text(strip=True) for cell in cells]
            print(row_data)
            if len(row_data) > 10:
                irdai_data.append(row_data)
                print(row_data)


def load_data():
    df = pd.DataFrame(irdai_data, columns=columns)
    df["ingestiontimestamp"] = str(datetime.now())
    df["validfrom"] = pd.to_datetime(df["validfrom"])
    df["validto"] = pd.to_datetime(df["validto"])

    print(df.head())
    host = "194.163.128.158"
    user = "amfi_user"
    database = "amfi_data"
    password = "BxH#X=eG[6r4s37h"

    db_config = {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
    }

    load_dataframe_to_mysql(dataframe, "amfi", db_config)


def extract_and_load_data(insurance_type_id, insurer_id, state_id, district_id):
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, args=["--start-maximized"])

        context = browser.new_context()

        page = context.new_page()
        url = "https://agencyportal.irdai.gov.in/PublicAccess/AgentLocator.aspx"
        page.goto(url)
        page.evaluate(
            '(document.getElementById("ddlInsurer")).removeAttribute("disabled")'
        )
        page.select_option("#ddlInsuranceType", insurance_type_id)
        page.select_option("#ddlInsurer", insurer_id)
        page.select_option("#ddlState", state_id)
        page.select_option("#ddlDistrict", district_id)
        page.click("#btnLocate")

        # element = page.wait_for_selector("#fgAgentLocator", state="visible")
        page_content = page.content()
        extract_data(page_content)
        browser.close()


def irdai_scraper_main():
    """Main function to orchestrate the scraping process."""
    print("IRDAI FETCHER VERSION 1.0")
    print("\n")
    _state_info = get_states()
    _get_insurer = get_insurer()

    for insurance_type_id, insurer_list in _get_insurer.items():
        for insurer_id in insurer_list:
            for state_id in _state_info.keys():
                for state_id, district_list in get_district(state_id).items():
                    for district_id in district_list:
                        extract_and_load_data(
                            insurance_type_id, insurer_id, state_id, district_id
                        )
                        if len(irdai_data) > 0:
                            load_data()
                            log.info(f"Loaded data for state_id {state_id} district_id {district_id}")

