import requests
import time
import json
import datetime
import os
import configparser

config_ini = 'config.ini'


def get_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def get_tickers_json(user_agent, company_tickers_dir_path):
    host = 'www.sec.gov'
    url = fr'https://{host}/files/company_tickers.json'
    out_path = os.path.join(company_tickers_dir_path, fr'{get_timestamp()}.json')

    headers = {
        "user-agent": user_agent,
        "accept-encoding": "gzip, deflate",
        "host": host}

    response = requests.get(url, headers=headers)

    tickers_json = response.json()

    with open(out_path, 'w') as outfile:
        json.dump(tickers_json, outfile)

    return tickers_json


def get_entity_filing_history(cik, user_agent, entity_filing_history_dir_path):
    out_path = os.path.join(entity_filing_history_dir_path, fr'CIK{cik}.json')
    headers = {
        "user-agent": user_agent,
        "accept-encoding": "gzip, deflate",
        "host": "data.sec.gov"}

    entity_filing_history_url = fr'https://data.sec.gov/submissions/CIK{cik}.json'
    response = requests.get(entity_filing_history_url, headers=headers)
    time.sleep(.25)

    response_json = response.json()
    with open(out_path, 'w') as outfile:
        json.dump(response_json, outfile)


def main():
    config = configparser.ConfigParser()
    config.read(config_ini)
    user_agent = config.get(section='DEFAULT', option='user_agent')
    company_tickers_dir_path = config.get(section='DEFAULT', option='company_tickers_dir_path')
    entity_filing_history_dir_path = config.get(section='DEFAULT', option='entity_filing_history_dir_path')

    tickers_json = get_tickers_json(user_agent=user_agent, company_tickers_dir_path=company_tickers_dir_path)

    # ~20% of the CIKs within this document are duplicates.
    # Maintain a list to track processed CIKs to save some time by preventing grabbing the same document twice
    processed_cik_list = set()

    for index, record in tickers_json.items():
        print(f'{datetime.datetime.now()} Processing {index}:{len(tickers_json)}\t{record}')
        cik = str(record['cik_str']).zfill(10)
        if cik in processed_cik_list:
            continue
        get_entity_filing_history(cik=cik, user_agent=user_agent,
                                  entity_filing_history_dir_path=entity_filing_history_dir_path)
        processed_cik_list.add(cik)


if __name__ == '__main__':
    main()
