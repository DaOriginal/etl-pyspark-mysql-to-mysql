import pandas as pd
import requests
import mysql.connector
import re

mysql_config = {
    'host': '',
    'user': '',
    'password': '',
    'database': ''
}


def process_api_data(data):
    data_to_str = str(data)

    reg_expression = r'\[(\d+(?:,\s*\d+)*)\]'

    patients_ids = re.findall(reg_expression, data_to_str)

    all_patients = []

    for list in patients_ids:
        all_patients.extend([int(id.strip()) for id in list.split(',')])

    return set(all_patients)


def get_patients_from_api(site_id, tx_indicator, start_date, end_date):
    api_url = f""
    response = requests.get(api_url)

    if response.status_code == 200:
        api_data = response.json()
        # df_api = pd.DataFrame(api_data)
        return process_api_data(api_data)
    else:
        print(f"Error: Unable to fetch data from API for site_id {site_id}")
        return None


def get_patients_from_db(site_id, tb_name):
    try:
        conn = mysql.connector.connect(**mysql_config)
        query = f"""
        SELECT patient_id 
        FROM {tb_name} 
        WHERE site_id = '{site_id}'; 
        """
        df_mysql = pd.read_sql(query, conn)
        conn.close()
        return set(df_mysql['patient_id'].tolist())
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return None


def patients_diff(df_api, df_mysql):

    patients_in_api_not_in_db = df_api - df_mysql
    patients_in_db_not_in_api = df_mysql - df_api

    if patients_in_api_not_in_db:
        print("patients in API but not in DB:")
        print(patients_in_api_not_in_db)
    else:
        print("No patients missing from DB")

    if patients_in_db_not_in_api:
        print("patients in DB but not in API:")
        print(patients_in_db_not_in_api)
    else:
        print("No patients missing from API")


def main(facility_id, indicator,table_name, start_date, end_date):
    df_api = get_patients_from_api(facility_id, indicator, start_date, end_date)

    df_mysql = get_patients_from_db(facility_id,table_name)

    if df_api is not None and df_mysql is not None:
        patients_diff(df_api, df_mysql)


if __name__ == '__main__':
    facility_id = ''
    indicator = ''
    table_name = ''
    start_date = ''
    end_date = ''

    main(facility_id,indicator ,table_name, start_date, end_date)
