from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

args = {'owner':'airflow',
	'start_date': datetime(2024,4,17)
	}
python_dag= DAG('python_dag',
		default_args=args,
		description='Practice Python DAG',
		catchup=False,
		schedule='@daily')

def python_fun():
	import json
	item_count={}
	with open('/tmp/transactions.csv') as file:
		for lines in file.readlines:
			line = lines.strip()
			if line=="":
				continue
			userid, item, start, end, amount=line.split(" ")
			total_count=item_count.get(item,0)+1
			item_count[item]=total_count
	with open('/tmp/output2.json ', 'w') as out_file:
		out_file.write(json.dumps(item_count, indent=2))

bash_command="""
	ls -l /tmp/transactions.csv;
	if [ $? -eq 0];
		then
			echo "File exists";
	else
		echo "File does not exists";
		exit 1
	fi"""

check_file=BashOperator(
	task_id='check_file',
	bash_command=bash_command,
	dag=python_dag)

analyze_file=PythonOperator(
	task_id='analyse_file',
	python_callable=python_fun,
	dag=python_dag)

check_file >> analyze_file
