import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import requests
from bs4 import BeautifulSoup

args = {'owner':'airflow',
	'start_date': datetime(2024,4,30)
	}

dag = DAG('web_scraping_dag',
	default_args=args,
	description='A Webscrapping DAG',
	catchup=False,
	schedule='@hourly')


def get_stock_info(stock_symbol):
    url = f"https://www.moneycontrol.com/india/stockpricequote/computers-software/{stock_symbol}"
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        stock_info = {}
        # Extracting stock name
        stock_info['Name'] = soup.find('div', class_='inid_name').text.strip()
        # Extracting stock price
        stock_info['Price'] = soup.find('div', class_='inprice1 nsecp').text.strip()
        # Extract stock details
        stock_info['Previous Close'] = soup.find('td', class_='nseprvclose bseprvclose').text.strip()
        stock_info['Open'] = soup.find('td', class_='nseopn bseopn').text.strip()
        stock_info['Volume'] = soup.find('td', class_='nsevol bsevol').text.strip()
        stock_info['Value(Lacs)'] = soup.find('td', class_='nsevalue bsevalue').text.strip()
        #stock_info['Beta(NSE)'] = soup.find('td', class_='nsebeta').text.strip()
        #stock_info['Beta(BSE)'] = soup.find('td', class_='bsebeta').text.strip()
        stock_info['Mkt Cap(Rs. Cr.)'] = soup.find('td', class_='nsemktcap bsemktcap').text.strip()
        stock_info['High'] = soup.find('td', class_='nseHP bseHP').text.strip()
        stock_info['Low'] = soup.find('td', class_='nseLP bseLP').text.strip()
        stock_info['52 Week High'] = soup.find('td', class_='nseH52 bseH52').text.strip()
        stock_info['52 Week Low'] = soup.find('td', class_='nseL52 bseL52').text.strip()
        stock_info['Face Value'] = soup.find('td', class_='nsefv bsefv').text.strip()
        stock_info['All Time High'] = soup.find('td', class_='nseLTH bseLTH').text.strip()
        stock_info['All Time Low'] = soup.find('td', class_='nseLTL bseLTL').text.strip()
        stock_info['Book Value Per Share'] = soup.find('td', class_='nsebv bsebv').text.strip()
        stock_info['Dividend Yield'] = soup.find('td', class_='nsedy bsedy').text.strip()
        # Extracting other relevant information
        # Add more code to extract other information like previous close, open, volume, etc.
        return stock_info
    else:
        print("Failed to retrieve data.")
        return None

def main():
    current_dir=os.getcwd()
    path=os.path.join(current_dir+'/dags/stocks.txt')
    arg1=open(path,'r').read()
    args=arg1.replace('\n','').split(',')
    for arg in args:
        print(get_stock_info(arg))


get_data = PythonOperator(
        task_id ='get_data',
        python_callable = main,
        dag=dag)
