from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime
import xmltodict
import logging
import requests
import json

from src.database import load_dataframe_to_mysql
from src.logs_utils import IRDALogData

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

record_counts = 0

with open(f"src/state_districts_mapping.json", 'r', encoding='utf-8') as file:
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
    global record_counts
    logging.info("Loading data")
    df["ingestiontimestamp"] = str(datetime.now())
    df["validfrom"] = pd.to_datetime(df["validfrom"], format='mixed', errors='coerce', dayfirst=True)
    df["validto"] = pd.to_datetime(df["validto"], format='mixed', errors='coerce', dayfirst=True)

    logging.info(df.head())
    record_counts +=len(df)

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

    if response.status_code == 200:
        data = xmltodict.parse(response.text)
        if "row" in data['rows']:
            rows = data['rows']['row']
            if data['rows']['total'] == '1':
                records = [rows['cell']]
            else:
                records = [row['cell'] for row in rows]
            logging.info(f"{insurance_type_id} {insurer_id} {state_id} {district_id}")
            df = pd.DataFrame(data=records, columns=columns)
            load_data(df)
    else:
        logging.error(f"Agent Locator error {response.status_code}")


def worker(insurance_type_id, insurer_data, state_id, district_list):

    for insurer_id in insurer_data:
        for district_id in district_list:
            agent_locator(insurance_type_id, insurer_id, state_id, district_id)
            


def irdai_scraper_main():
    logging.info("IRDAI FETCHER VERSION 1.0")
    logging.info("/n")
    insurer_type_info = get_insurer_type()
    insurer_data = get_insurer()
    global record_counts

    with ThreadPoolExecutor(max_workers=64) as executor:
        tasks = []
        for insurance_type_id, insurer_list in insurer_data.items():
            for state_id, district_list in get_district().items():
                try:
                    task = executor.submit(worker, insurance_type_id, insurer_list, state_id, district_list)
                    tasks.append(task)
                except Exception as err:
                    logging.error(f"Exception : {err}")
                    break

        # make sure all tasks are completed
        for future in tasks:
            future.result()

    logging.info("Scraping Completed!")


def main():
    JobStart = datetime.now().replace(second=0, microsecond=0)

    try:
        irdai_scraper_main()
        description = f"Total records addedd/Upsert {record_counts}"
        completed = 1
        _error = None
        JobEnd = datetime.now().replace(second=0, microsecond=0)
        logsdriver = IRDALogData(description, completed, _error, JobStart, JobEnd)
        logsdriver.load_logs_data()
        logging.info(f"{JobStart} {JobEnd} {description} {_error} {completed}")
    except Exception as err:
        _error = str(err)
        logging.error(_error)
        description = f"Error Total records addedd/Upsert {record_counts}"
        completed = 0
        JobEnd = datetime.now()
        logsdriver = IRDALogData(description, completed, _error.replace("()", "").replace(",", ""), JobStart, JobEnd)
        logsdriver.load_logs_data()
        logging.info(f"{JobStart} {JobEnd} {description} {_error} {completed}")