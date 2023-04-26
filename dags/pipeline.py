from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
from bson.son import SON

# Define the MongoDB connection parameters
MONGO_CONN_ID = 'my_mongo_conn'
MONGO_DB = 'my_database'

# Define the scripts to run
QUERY_SCRIPTS = ['script1.py', 'script2.py', 'script3.py', 'script4.py']
AGGREGATION_SCRIPT = 'aggregation_script.py'
CLEANUP_SCRIPT = 'cleanup_script.py'
TRANSFORMATION_SCRIPT = 'transformation_script.py'

# Define the default arguments for the DAG
default_args = {
    'owner': 'yourname',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 25),
}

# Create the DAG object
dag = DAG(
    'my_pipeline',
    default_args=default_args,
    description='A pipeline that queries data from MongoDB and stores the results in a new collection',
    schedule_interval='@daily',
)

# Define the functions to run the scripts
def run_query_script(script, query_param):
    # Connect to MongoDB
    mongo = MongoClient()
    db = mongo[MONGO_DB]
    # Run the script
    exec(open(script).read(), {'query_param': query_param})
    # Close the connection
    mongo.close()

def run_aggregation_script():
    # Connect to MongoDB
    mongo = MongoClient()
    db = mongo[MONGO_DB]
    # Run the script
    exec(open(AGGREGATION_SCRIPT).read())
    # Close the connection
    mongo.close()

def run_cleanup_script():
    # Connect to MongoDB
    mongo = MongoClient()
    db = mongo[MONGO_DB]
    # Run the script
    exec(open(CLEANUP_SCRIPT).read())
    # Close the connection
    mongo.close()

def run_transformation_script():
    # Connect to MongoDB
    mongo = MongoClient()
    db = mongo[MONGO_DB]
    # Run the script
    exec(open(TRANSFORMATION_SCRIPT).read())
    # Close the connection
    mongo.close()

# Create the tasks for the DAG
query_tasks = []
for query_script in QUERY_SCRIPTS:
    task = PythonOperator(
        task_id=f'run_{query_script}',
        python_callable=run_query_script,
        op_kwargs={'script': query_script, 'query_param': 'your_query_param'},
        dag=dag,
    )
    query_tasks.append(task)

aggregation_task = PythonOperator(
    task_id='run_aggregation_script',
    python_callable=run_aggregation_script,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='run_cleanup_script',
    python_callable=run_cleanup_script,
    dag=dag,
)

transformation_task = PythonOperator(
    task_id='run_transformation_script',
    python_callable=run_transformation_script,
    dag=dag,
)

# Set up the dependencies between tasks
for query_task in query_tasks:
    query_task >> aggregation_task
aggregation_task >> cleanup_task >> transformation_task
