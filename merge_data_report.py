from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import requests
from airflow.models import Variable
from datetime import datetime, timezone
import re
try:
    import boto3
except:
    os.system('pip install boto3')
    import boto3
try:
    import pandas as pd
except:
    os.system('pip install pandas')
    import pandas as pd
try:
    import mysql.connector
except:
    os.system('pip install mysql-connector-python')
    import mysql.connector
    
def connect_database(**kwargs):
    # lấy connect string từ biến airflow
    connect_string = Variable.get('database_raw')
    pattern = r"mysql\+mysqlconnector:\/\/(.*?):(.*?)@(.*?):(.*?)\/(.*)"
    match = re.match(pattern, connect_string)
    if match:
        user = match.group(1)
        password = match.group(2)
        host = match.group(3)
        port = match.group(4)
        database = match.group(5)
        
    conn = mysql.connector.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )
    if conn.is_connected():
        return conn
    else:
        raise ValueError("Kết nối không thành công")

def client_s3():
    s3_client = boto3.client(
        's3',
        aws_access_key_id= Variable.get('aws_access_key_id'),
        aws_secret_access_key= Variable.get('aws_secret_access_key')
    )
    return s3_client


def read_all_files(bucket, **kwargs):
    s3_client = client_s3()
    response = s3_client.list_objects_v2(Bucket=bucket)
    files = response.get('Contents', [])
    all_files = []
    for file in files:
        all_files.append(file['Key'])
    print(all_files)
    kwargs['ti'].xcom_push(key='all_files', value=all_files)
    


def fillter_recent_files(time_limit = timedelta(days=1), **kwargs):
    s3_client = client_s3()
    all_files = kwargs['ti'].xcom_pull(task_ids='read_all_files', key='all_files')
    today = datetime.now(timezone.utc).date()
    today_start = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)
    recent_files = []
    for file in all_files:
        upload_time = s3_client.head_object(Bucket='iart-data', Key=file)['LastModified']
        if upload_time >= today_start - time_limit:
            recent_files.append(file)
    print(recent_files)
    kwargs['ti'].xcom_push(key='recent_files', value=recent_files)


def read_recent_file(**kwargs):
    s3_client = client_s3()
    recent_files = kwargs['ti'].xcom_pull(task_ids='fillter_recent_files', key='recent_files')
    for file in recent_files:
        response = s3_client.get_object(Bucket='iart-data', Key=file)
        df = pd.read_csv(response['Body'])
        print(df)
        kwargs['ti'].xcom_push(key=file, value=df)
        
def transform_data_date_ranger_report(**kwargs):
    data = {}
    recent_files = kwargs['ti'].xcom_pull(task_ids='fillter_recent_files', key='recent_files')
    for file in recent_files:
        df = kwargs['ti'].xcom_pull(task_ids='read_recent_file', key=file)
        # đổi tên cột thành chữ thường và replace khoảng trắng và / bằng dấu _ 
        df.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('/', '_').replace("'", '_').replace(':','') for col in df.columns]
        kwargs['ti'].xcom_push(key=file, value=df)
        
        company = file.split('/')[0]
        platform = file.split('/')[1]
        account = file.split('/')[2]
        region = file.split('/')[3]
        
        # ghép tất cả các cột thành 1 chuỗi sau đó hash thành 1 chuỗi duy nhất
        df['hash'] = df.apply(lambda x: hash(''.join([str(x[col]) for col in df.columns])), axis=1)
        
        # thêm cột account vào df
        df['account'] = account
        # thêm cột region vào df
        df['region'] = region
        # thêm cột company vào df
        df['company'] = company
        # thêm cột platform vào df
        df['platform'] = platform
        # thêm cột xpath vào df
        df['xpath'] = file
        
        # tạo 1 bảng f'company_platform_date_range_report_region
        table_name = f'{company}_{platform}_date_range_report_{region}'
        if table_name not in data.keys():
            data[table_name] = []
        data[table_name].append(df)
        
        
        
        
    con = connect_database()
    cursor = con.cursor()
    
    for table_name, dfs in data.items():
        columns = dfs[0].columns
        
        # tạo bảng nếu chưa tồn tại với tên cột là tên cột của df
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} TEXT' for col in columns])})"
        cursor.execute(query)
    

        
        
        
    for table_name, dfs in data.items():
        columns = dfs[0].columns
        
        # lọc ra những row có hash chưa tồn tại trong db
        query = f"SELECT hash FROM {table_name}"
        cursor.execute(query)
        hash_in_db = cursor.fetchall()
        hash_in_db = [x[0] for x in hash_in_db]
        new_data = []
        for i, df in enumerate(dfs):
            new_data.append(df[~df['hash'].isin(hash_in_db)])
           
        print(new_data)
         
        # thêm dữ liệu mới vào db
        for i, df in enumerate(new_data):
            for index, row in df.iterrows():
                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join([f'"{row[col]}"' for col in columns])})"
                cursor.execute(query)
                
                
    con.commit()
    cursor.close()
    con.close()
        
        

    
    
    
        


default_args = {
    'owner': 'hoangks5',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'scan_recent_file',
    default_args=default_args,
    description='Scan recent file in s3',
    schedule_interval='@daily',
    catchup=False,
    tags= ['amazon']
) as dag:
    read_all_files = PythonOperator(
        task_id='read_all_files',
        python_callable=read_all_files,
        op_args=['iart-data']
    )

    fillter_recent_files = PythonOperator(
        task_id='fillter_recent_files',
        python_callable=fillter_recent_files
    )

    read_recent_file = PythonOperator(
        task_id='read_recent_file',
        python_callable=read_recent_file
    )
    
    transform_data_date_ranger_report = PythonOperator(
        task_id='transform_data_date_ranger_report',
        python_callable=transform_data_date_ranger_report
    )
    
    read_all_files >> fillter_recent_files >> read_recent_file >> transform_data_date_ranger_report
    