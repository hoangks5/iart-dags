from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Hàm Python sẽ được thực thi
def hello_world():
    print("Hello, World!")

def print_date():
    print(datetime.now())

# Định nghĩa default_args (các tham số mặc định) cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Tạo DAG
with DAG(
    'basic_dag',  # Tên của DAG
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),  # Lịch chạy hàng ngày
    catchup=False,  # Đảm bảo DAG chỉ chạy từ thời điểm hiện tại
) as dag:

    # Tạo một task sử dụng PythonOperator
    hello_task = PythonOperator(
        task_id='hello_task',  # Tên task
        python_callable=hello_world,  # Hàm sẽ được gọi
    )

with DAG(
    'print_date_dag',  # Tên của DAG
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),  # Lịch chạy hàng ngày
    catchup=False,  # Đảm bảo DAG chỉ chạy từ thời điểm hiện tại
) as dag:
    print_date_task = PythonOperator(
        task_id='print_date_task',  # Tên task
        python_callable=print_date,  # Hàm sẽ được gọi
    )

hello_task >> print_date_task
