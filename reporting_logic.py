import mysql.connector
import pandas as pd
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def connect_to_db():
    """Connects to a MySQL database"""
    hostname = ""
    username = ""
    password = ""
    database = ""
    try:
        connection = mysql.connector.connect(
            host=hostname, user=username, password=password, database=database,
            connection_timeout=600, autocommit=False)
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to database: {err}")
        return None


def get_successful_sites(connection):
    cursor = connection.cursor()
    try:
        query = """
                    select distinct id from <main_table>;
                               """
        cursor.execute(query)
        return cursor.fetchall()
    except mysql.connector.Error as err:
        logging.error(f"Error executing query: {err}")
        return None

def drop_create_tables(connection,file_name):
    try:
        with open(file_name, 'r') as sql_file:
            get_query = sql_file.read()
            cursor = connection.cursor()
            for query in get_query.split(';'):
                query = query.strip().rstrip('--')
                if query:
                    print(f"Dropping and Creation of tables done!")
                    cursor.execute(query)
    except mysql.connector.Error as err:
        print(f"Here Error executing query: {err}")
        return None  


def insert_records_into_tables(site_id, file_name, start_date, end_date, max_retries=10):
    retry_count = 0
    while retry_count < max_retries:
        connection = connect_to_db()
        if connection:
            try:
                with open(file_name, 'r') as sql_file:
                    get_query = sql_file.read()
                    cursor = connection.cursor()
                    for query in get_query.split(';'):
                        query = query.strip().rstrip('--')
                        if query:
                            logging.info(f"Inserting records for {site_id}")
                            format_query = query.format(
                                site_id=site_id, start_date=start_date, end_date=end_date)
                            cursor.execute(format_query)
                            connection.commit()
                    logging.info(
                        f"Records inserted successfully for {site_id}")
                    logging.info(
                        f"Retry count {retry_count} on site {site_id}")
                    cursor.close()
                    return
            except mysql.connector.Error as err:
                logging.error(
                    f"Error executing query under insert_records_into_tables function: {err}")
                if err.errno == 1213:
                    retry_count += 1
                    wait_time = 15 ** retry_count
                    logging.info(
                        f"Deadlock detected. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    retry_count += 1
                    wait_time = 15 ** retry_count
                    logging.info(
                        f"Retry count {retry_count} and wait time is {wait_time} on site {site_id}")
                    time.sleep(wait_time)
                    break
            finally:
                connection.close()
        else:
            logging.error("Could not connect to the database.")
            break
    logging.error(
        f"Failed to insert records for {site_id} after {max_retries} retries.")


def calculate_tx_indicator(site_id, file_name, start_date,end_date):
    connection = connect_to_db()
    if connection:
        try:
            with open(file_name, 'r') as sql_file:
                get_query = sql_file.read()
                cursor = connection.cursor()
                for query in get_query.split(';'):
                    query = query.strip().rstrip('--')
                    if query:
                        format_query = query.format(site_id=site_id, start_date=start_date,end_date=end_date)
                        cursor.execute(format_query)
                        data = cursor.fetchall()
                        if data:
                            if os.path.basename(file_name) == 'report_file1.sql':
                                logging.info(f"Running REPORT_1 for {site_id}")
                                columns = ["<column1>", "<column2>", "<column3>", "<column4>", "<column5>", "<column6>"]
                                df = pd.DataFrame([row for row in data if not any(
                                    cell is None for cell in row)], columns=columns)
                                logging.info(
                                    f"Done running REPORT_1 report for {site_id}")
                                df.to_csv(
                                    f"./final_pull/REPORT_1_{site_id}.results.csv", index=False)

                            elif os.path.basename(file_name) == 'report_file2.sql':
                                logging.info(f"Running REPORT_2 for {site_id}")
                                columns = ["<column1>", "<column2>", "<column3>", "<column4>", "<column5>", "<column6>"]
                                df = pd.DataFrame([row for row in data if not any(
                                    cell is None for cell in row)], columns=columns)
                                logging.info(
                                    f"Done running REPORT_2 report for {site_id}")
                                df.to_csv(
                                    f"./final_pull/REPORT_2_{site_id}.results.csv", index=False)
                                                                                   
            cursor.close()
        except mysql.connector.Error as err:
            logging.error(
                f"Error executing query under calculate_tx_indicator function: {err}")
            return None
        finally:
            connection.close()
    else:
        logging.error("Could not connect to the database.")
        return None


def main():
    connection = connect_to_db()
    if connection:
        logging.info("Connected successfully!")
        sql_file_names = ["./report_file1.sql","./report_file2.sql","./report_file3.sql","./report_file4.sql"]
        insert_all_tables_1 = './insert_all_tables.sql'
        drop_tables = './truncate_tables.sql'
        sites_data = get_successful_sites(connection)
        drop_create_tables(connection, drop_tables)
        connection.close()

        if sites_data:
            start_time = time.time()
            column = ["site_id"]
            start_date_str = ''
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date_str = ''
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            df = pd.DataFrame(sites_data, columns=column)

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for index, row in df.iterrows():
                    site_id = row['site_id']
                    futures.append(executor.submit(
                        insert_records_into_tables, site_id, insert_all_tables_1, start_date, end_date))

                for future in as_completed(futures):
                    result = future.result()
                    if result is not None:
                        logging.info(f"Insert task completed successfully")
                    else:
                        logging.error(f"Insert task returned no data")

                futures.clear()

                for sql_file_name in sql_file_names:
                    for index, row in df.iterrows():
                        site_id = row['site_id']
                        futures.append(executor.submit(
                            calculate_tx_indicator, site_id, sql_file_name, start_date,end_date))

                for future in as_completed(futures):
                    result = future.result()
                    if result is not None:
                        logging.info(f"Reports task completed successfully")
                    else:
                        logging.error(f"Reports task returned no data")

            end_time = time.time()
            total_time = end_time - start_time
            hours, remainder = divmod(total_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            logging.info(
                f"Total time taken for inserting data and pulling reports was {hours} hours, {minutes} minutes, and {seconds:.2f} seconds.")


if __name__ == "__main__":
    main()
