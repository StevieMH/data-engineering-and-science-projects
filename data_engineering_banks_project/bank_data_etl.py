"""ETL pipeline to extract, transform, and load largest bank data from Wikipedia."""

from datetime import datetime
import sqlite3
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

DATA_URL = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
TABLE_ATTRIBUTES_EXTRACTION = ['Name', 'MC_USD_Billion']
TABLE_ATTRIBUTES_FINAL = [
    'Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion'
]
CSV_PATH = 'Largest_banks_data.csv'
DB_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'
LOG_FILE = 'code_log.txt'
EXCHANGE_RATE_CSV_PATH = 'exchange_rate.csv'


def log_progress(message):
    """Log messages with timestamps to a file."""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(LOG_FILE, 'a', encoding='utf-8') as log:
        log.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    """Extract bank data from the webpage into a DataFrame."""
    dataframe = pd.DataFrame(columns=table_attribs)

    html_page = requests.get(url, timeout=10).text
    soup = BeautifulSoup(html_page, 'html.parser')

    table = soup.find_all('tbody')[0]
    rows = table.find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            bank_name = cols[1].text.strip()
            market_cap = cols[2].text.strip()

            dataframe = pd.concat(
                [dataframe, pd.DataFrame([{
                    'Name': bank_name,
                    'MC_USD_Billion': market_cap
                }])],
                ignore_index=True
            )

    return dataframe


def transform(dataframe, exchange_path):
    """Convert market cap values to GBP, EUR, and INR using exchange rates."""
    exchange_rate_df = pd.read_csv(exchange_path)
    exchange_dict = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    dataframe['MC_USD_Billion'] = pd.to_numeric(
        dataframe['MC_USD_Billion'], errors='coerce')
    dataframe['MC_GBP_Billion'] = [
        np.round(x * exchange_dict['GBP'], 2) for x in dataframe['MC_USD_Billion']
    ]
    dataframe['MC_EUR_Billion'] = [
        np.round(x * exchange_dict['EUR'], 2) for x in dataframe['MC_USD_Billion']
    ]
    dataframe['MC_INR_Billion'] = [
        np.round(x * exchange_dict['INR'], 2) for x in dataframe['MC_USD_Billion']
    ]

    return dataframe


def load_to_csv(dataframe, output_path):
    """Save DataFrame to CSV."""
    dataframe.to_csv(output_path, index=False)


def load_to_db(dataframe, sql_connection, table_name):
    """Save DataFrame to SQLite database."""
    dataframe.to_sql(table_name, sql_connection,
                     if_exists='replace', index=False)


def run_query(query, sql_connection):
    """Run and print SQL queries."""
    print('-' * 80 + '\n' + query + '\n' + '-' * 80)
    output = pd.read_sql(query, sql_connection)
    print(output)
    print('\n' + '\n')


def main():
    """Main ETL execution logic."""
    log_progress('Preliminaries complete. Initiating ETL process.')

    extracted_df = extract(DATA_URL, TABLE_ATTRIBUTES_EXTRACTION)
    log_progress('Data extraction complete. Initiating Transformation process')

    transformed_df = transform(extracted_df, EXCHANGE_RATE_CSV_PATH)
    log_progress('Data transformation complete. Initiating loading process')

    load_to_csv(transformed_df, CSV_PATH)
    log_progress('Data saved to CSV file')

    conn = sqlite3.connect(DB_NAME)
    log_progress('SQL Connection initiated')

    load_to_db(transformed_df, conn, TABLE_NAME)
    log_progress('Data loaded to Database as a table, Executing queries')

    query_statement_1 = 'SELECT * FROM Largest_banks'
    query_statement_2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
    query_statement_3 = 'SELECT Name FROM Largest_banks LIMIT 5'

    run_query(query_statement_1, conn)
    run_query(query_statement_2, conn)
    run_query(query_statement_3, conn)

    log_progress('Process Complete.')
    conn.close()


if __name__ == '__main__':
    main()
