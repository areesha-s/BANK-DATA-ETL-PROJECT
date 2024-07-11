import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from io import StringIO
import matplotlib.pyplot as plt

def setup_logging():
    logging.basicConfig(
        filename='etl_process.log',
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )

setup_logging()
logging.info('Logging setup complete.')

def extract_data():
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    response = requests.get(url)

    if response.status_code == 200:
        logging.info('Successfully fetched the webpage.')
    else:
        logging.error('Failed to fetch the webpage.')
        return None
    
    soup = BeautifulSoup(response.content, 'html.parser')
    tables = soup.find_all('table', {'class': 'wikitable'})
    table = tables[0]
    return table

table = extract_data()
logging.info('Data extraction complete.')

def transform_data(table):
    table_html = str(table)
    df = pd.read_html(StringIO(table_html))[0]
    
    df.columns = ['Rank', 'Bank name', 'Market cap (US$ billion)']
    
    # Convert 'Market cap (US$ billion)' to string before replacing commas
    df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].astype(str).str.replace(',', '').astype(float)
    
    exchange_rates = pd.read_csv('exchange_rate.csv')
    if 'Currency' not in exchange_rates.columns or 'Rate' not in exchange_rates.columns:
        raise KeyError("The exchange_rate.csv file must contain 'Currency' and 'Rate' columns.")
    rates = dict(zip(exchange_rates['Currency'], exchange_rates['Rate']))
    
    df['Market cap (INR Billion)'] = df['Market cap (US$ billion)'] * rates['INR']
    df['Market cap (EUR Billion)'] = df['Market cap (US$ billion)'] * rates['EUR']
    df['Market cap (GBP Billion)'] = df['Market cap (US$ billion)'] * rates['GBP']
    
    return df

df = transform_data(table)
logging.info('Data transformation complete.')

def load_to_csv(df, filename='transformed_banks_data.csv'):
    df.to_csv(filename, index=False)
    logging.info(f'Data loaded to CSV file: {filename}')

load_to_csv(df)

def load_to_db(df, db_name='new_banks.db'):
    conn = sqlite3.connect(db_name)
    df.to_sql('banks', conn, if_exists='replace', index=False)
    conn.close()
    logging.info('Data loaded to SQLite database.')

load_to_db(df)

def run_queries(db_name='new_banks.db'):
    conn = sqlite3.connect(db_name)
    
    query1 = pd.read_sql_query("SELECT * FROM banks ORDER BY `Market cap (INR Billion)` DESC LIMIT 5", conn)
    query2 = pd.read_sql_query("SELECT AVG(`Market cap (US$ billion)`) as Avg_Market_Cap FROM banks", conn)
    query3 = pd.read_sql_query("SELECT * FROM banks WHERE `Market cap (INR Billion)` > 5000", conn)
    
    conn.close()
    
    logging.info('Queries executed successfully.')
    return query1, query2, query3

query1, query2, query3 = run_queries()
print("Top 5 Banks by Market Cap in INR:")
print(query1)
print("\nAverage Market Cap in US$ billion:")
print(query2)
print("\nBanks with Market Cap > 5000 Billion INR:")
print(query3)

def plot_query_results(query1, query2, query3):
    # Plot query1: Top 5 Banks by Market Cap in INR
    plt.figure(figsize=(10, 6))
    plt.barh(query1['Bank name'], query1['Market cap (INR Billion)'], color='skyblue')
    plt.xlabel('Market Cap (INR Billion)')
    plt.title('Top 5 Banks by Market Cap in INR')
    plt.gca().invert_yaxis()  # Invert y-axis to display highest market cap at the top
    plt.tight_layout()
    plt.savefig('query1_top_banks_market_cap.png')
    plt.show()

    # Plot query2: Average Market Cap in US$ billion
    plt.figure(figsize=(6, 4))
    plt.bar(['Average Market Cap'], query2['Avg_Market_Cap'], color='lightgreen')
    plt.ylabel('Average Market Cap (US$ billion)')
    plt.title('Average Market Cap')
    plt.tight_layout()
    plt.savefig('query2_avg_market_cap.png')
    plt.show()

    # Plot query3: Banks with Market Cap > 5000 Billion INR
    plt.figure(figsize=(10, 6))
    plt.barh(query3['Bank name'], query3['Market cap (INR Billion)'], color='lightcoral')
    plt.xlabel('Market Cap (INR Billion)')
    plt.title('Banks with Market Cap > 5000 Billion INR')
    plt.gca().invert_yaxis()  # Invert y-axis to display highest market cap at the top
    plt.tight_layout()
    plt.savefig('query3_high_market_cap_banks.png')
    plt.show()

plot_query_results(query1, query2, query3)

def verify_logs():
    with open('etl_process.log', 'r') as f:
        logs = f.read()
        print(logs)

verify_logs()
