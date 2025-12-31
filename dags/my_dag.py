# importing the libraries
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from random import randint

# defining a function that returns fake accuracy
def _training_model():
    return randint(1,10)

# defining a function that prints the random accuracies generated
def _print_accuracies(ti):
    accuracies=ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])

    print(f"Model Accuracies: {accuracies}")

# defining a function that chooses the best accuracy
def _choose_best_model(ti):
    accuracies=ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])

    best_accuracy=max(accuracies)

    if (best_accuracy>8):
        return 'accurate'
    return 'inaccurate'

# defining the DAG
with DAG("my_dag", start_date=datetime(2025,12,1), schedule_interval="@daily", catchup=False) as dag:
    
    # defining the start node
    start=EmptyOperator(
        task_id="start"
    )
    
    # defining the first task
    training_model_A=PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model
    )
    
    # defining the second task
    training_model_B=PythonOperator(
            task_id="training_model_B",
            python_callable=_training_model
        )
    
    # defining the third task
    training_model_C=PythonOperator(
        task_id="training_model_C",
        python_callable=_training_model
    )

    # defining the task that prints the random accuracies generated
    print_accuracies=PythonOperator(
        task_id="print_accuracies",
        python_callable=_print_accuracies
    )

    # defining the task that calls a function to choose the best accuracy from A,B and C, and hence decides which task to run next
    choose_best_model=BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model
    )

    # defining the task to run if function returns accurate
    accurate=BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    # defining the task to run if function returns inaccurate
    inaccurate=BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

    # defining the end node
    end=EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    start >> [training_model_A, training_model_B, training_model_C] >> print_accuracies >> choose_best_model >> [accurate, inaccurate] >> end