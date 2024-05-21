import os
import sys
import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.operators.bash import BashOperator
#from airflow.models.xcom_arg import XComArg

args = {'owner': 'airflow', 'start_date': datetime(2024,5,1)}

dag=DAG('websc_tofile_dag',
        default_args=args,
        description="Dag for saving capture of data and pass through xcom",
        catchup=True,
        schedule='@daily')

def main(ti):
    cwd= os.getcwd()
    print(cwd)
    path=os.path.join(cwd,'dags/stocks.txt')
    file=open(path,'r').read().replace('\n','')
    arg=file.split(',')
    #print(arg)
    ti.xcom_push(key='Stocks',value=arg)
    #return arg

get_stock_names= PythonOperator(
        task_id='get_stock_names',
        python_callable=main,
        dag=dag)

def get_stock_info(ti):
    stock_symbols= ti.xcom_pull(key='Stocks',task_ids='get_stock_names')
    print(stock_symbols)
    stock_details=[]
    for stock_symbol in stock_symbols:
        url=f"https://www.moneycontrol.com/india/stockpricequote/computers-software/{stock_symbol}"
        response=requests.get(url)
        if response.status_code==200:
            soup=BeautifulSoup(response.content,'html.parser')
            stock_info={}
            # Extracting stock name
            stock_info['Name'] = soup.find('div', class_='inid_name').text.strip()
            # Extracting stock price
            stock_info['Price'] = soup.find('div', class_='inprice1 nsecp').text.strip()
            #Extract stock details
            stock_info['Previous Close'] = soup.find('td', class_='nseprvclose bseprvclose').text.strip()
            stock_info['Open'] = soup.find('td', class_='nseopn bseopn').text.strip()
            stock_info['Volume'] = soup.find('td', class_='nsevol bsevol').text.strip()
            stock_info['Value(Lacs)'] = soup.find('td', class_='nsevalue bsevalue').text.strip()
            # stock_info['Beta(NSE)'] = soup.find('td', class_='nsebeta').text.strip()
            # stock_info['Beta(BSE)'] = soup.find('td', class_='bsebeta').text.strip()
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
            stock_details.append(stock_info)
        else:
            print("failed to retrieve data")
            return None
    return stock_details

stock_information=PythonOperator(
        task_id="stock_information",
        python_callable=get_stock_info,
        dag=dag)

bash_check_file="""
    path1=~/airflow/dags/tcs.csv
    path2=~/airflow/dags/infosys.csv
    path3=~/airflow/dags/wipro.csv
    ls -l $path1
    if [ $? -eq 0 ];
    then
      echo "TCS File Exists";
    else
      echo "TCS File does not exists";
      touch $path1
      echo "TCS File created";
    fi
    ls -l $path2
    if [ $? -eq 0 ];
    then
      echo "Infosys File Exists";
    else
      echo "Infosys File does not exists";
      touch $path2
      echo "Infosys file created";
    fi
    ls -l $path3
    if [ $? -eq 0 ];
    then
      echo "Wipro File Exists";
    else
      echo "Wipro File does not exists";
      touch $path3
      echo "Wipro File created";
      exit 0
    fi
    path_list=($path1, $path2, $path3)
    echo $path_list"""
###

check_file=BashOperator(
    task_id='check_file',
    bash_command=bash_check_file,
    dag=dag)

def write_data(ti):
    data=ti.xcom_pull(task_ids='stock_information')
    print(data)
    path=ti.xcom_pull(task_ids='check_file')
    for n in range(len(data)):
        f=open(path[n],'w')
        fields=data[n].keys()
        values=data[n].values()
        writer=csv.DictWriter(f,fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)
        f.close()

save_data=PythonOperator(
    task_id='save_data',
    python_callable=write_data,
    dag=dag
)

get_stock_names>>stock_information>>check_file>>save_data
