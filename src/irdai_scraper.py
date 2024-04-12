from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime
import xmltodict
import logging
import requests
from src.database import load_dataframe_to_mysql
import json

logging.basicConfig(
    filename="irdai_scraper.log",
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d-%b-%Y %H:%M:%S"
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
]

with open(f"src\state_districts_mapping.json", 'r', encoding='utf-8') as file:
    data = json.loads(file.read())


def get_district():
    return data['insurer_district_mapping']


def get_insurer_type():
    insurer_type_info = {}
    url = "https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetInsurerType"
    headers = {"Content-Type": "application/xml; charset=utf-8"}
    data = "{}"
    response = requests.post(url, headers=headers, data=data)
    data = xmltodict.parse(response.text)
    table_data = data["NewDataSet"]["Table"]
    for data in table_data:
        insurer_type_info[data["BintParamConstantValue"]] = data["VcParamValueDisplay"]
    return insurer_type_info


def get_insurer():
    insurer_info = {}
    url = "https://agencyportal.irdai.gov.in/_WebService/General/DataLoader.asmx/GetInsurer"
    headers = {"Content-Type": "application/xml; charset=utf-8"}

    _get_insurer_type = get_insurer_type()
    for insurance_type_id, insurance_type_name in _get_insurer_type.items():
        params = {"InsuranceType": insurance_type_id}
        response = requests.get(url, headers=headers, params=params)
        data = xmltodict.parse(response.text)
        table_data = data["NewDataSet"]["Table"]
        insurer_info[insurance_type_id] = [
            (insurer_data["intTblMstInsurerUserID"], insurer_data["varInsurerID"])
            for insurer_data in table_data
        ]
    return insurer_info


def load_data(df):
    df["ingestiontimestamp"] = str(datetime.now())
    df["validfrom"] = pd.to_datetime(df["validfrom"])
    df["validto"] = pd.to_datetime(df["validto"])

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
    logging.info(f"Loading {len(df)} in DB")
    load_dataframe_to_mysql(df, "irda", db_config)


def agent_locator(insurance_type_id, insurer_id, state_id, district_id):
    url = "https://agencyportal.irdai.gov.in/_WebService/PublicAccess/AgentLocator.asmx/LocateAgent"

    headers = {
        "accept": "application/xml, text/xml, */*",
        "content-type": "application/x-www-form-urlencoded"
    }

    data = {
        "page": "1",
        "rp": "9999",
        "sortname": "AgentName",
        "sortorder": "asc",
        "query": "",
        "qtype": "",
        "customquery": f",,,{insurance_type_id},{insurer_id[0]},{state_id},{district_id},"
    }
    response = requests.post(url, headers=headers, data=data)
    data = xmltodict.parse(response.text)
    if "row" in data['rows']:
        rows = data['rows']['row']
        records = [row['cell'] for row in rows]
        df = pd.DataFrame(data=records, columns=columns)
        load_data(df)


def worker(insurance_type_id, insurer_data, state_id, district_list):
    for insurer_id in insurer_data:
        for district_id in district_list:
            agent_locator(insurance_type_id, insurer_id, state_id, district_id)
            print(f"{insurance_type_id} {insurer_id[0]} {state_id} {district_id}")


def irdai_scraper_main():
    logging.info("IRDAI FETCHER VERSION 1.0")
    logging.info("/n")
    insurer_type_info = get_insurer_type()
    insurer_data = get_insurer()

    with ThreadPoolExecutor(max_workers=8) as executor:
        tasks = []
        for insurance_type_id, insurer_list in insurer_data.items():
            for state_id, district_list in get_district().items():
                try:
                    task = executor.submit(worker, insurance_type_id, insurer_list, state_id, district_list)
                    tasks.append(task)
                except:
                    pass

        # make sure all tasks are completed
        for future in tasks:
            future.result()

    logging.info("Scraping Completed!")
    
