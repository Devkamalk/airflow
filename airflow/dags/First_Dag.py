from airflow import DAG

from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Declaring and defining default dag property
default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2023,4,12),
    'retries':1
}

# passing default parameters for dag creation and scheduling to run every minute
dag = DAG(dag_id='first_dag',default_args=default_args,catchup=False,schedule_interval='* * * * *')


# Creating a python operator for task 2
def pythonOperator_task2():
    print('**************Hello from airflow python operator task 2********************')
    for i in range(10):
        print("task2 in progress")

# Creating a python operator for task 3
def pythonOperator_task3():
    print("=========================Hello from airflow python operator task 3======================================")
    for i in range(10):
        print("task3 in progress")
        
# Creating a task1 as dummy task operator for demo
DummyTask = DummyOperator(task_id='task1',dag=dag)# dummy task 

# Creating task2 and calling pythonOperator_task2
python1 = PythonOperator(
    task_id='task2',
    dag=dag,
    python_callable= pythonOperator_task2)

# Creating task3 and calling pythonOperator_task2
python2 = PythonOperator(
    task_id='task3',
    dag=dag,
    python_callable=pythonOperator_task3
)


#Defining task order and linking 
DummyTask >> python1>>python2