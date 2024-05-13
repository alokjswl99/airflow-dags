from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 11)
}

sample_bash_dag = DAG(
    'sample_bash_dag',
    default_args=default_args,
    description='Demo of Bash Operator',
    max_active_runs=1,
    catchup=True,
    schedule="0 11 * * *"
)

"""
Ensure user does not exist before
  sudo id -u admin
  sudo userdel admin
"""

bash_commands = """
    id -u admin;
    if [ $? -eq 0 ]; 
      then echo "User Exists"; 
    else sudo useradd -r admin; 
      echo "User Created"; 
    fi"""

create_user = BashOperator(
    task_id='create_user',
    bash_command=bash_commands,
    dag=sample_bash_dag)

