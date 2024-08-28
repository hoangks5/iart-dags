from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from airflow.models import Variable
import re
try:
    import numpy as np
except:
    os.system('pip install numpy')
    import numpy as np
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

def get_data_date_range_report():
    conn = connect_database()
    cursor = conn.cursor()
    
    # lấy dữ liệu sku và product_name từ bảng sku_productname
    cursor.execute("SELECT * FROM sku_productname")
    rows = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    df_sku_productname = pd.DataFrame(rows, columns=columns)
    # loại bỏ trùng lặp giá trị sku
    df_sku_productname = df_sku_productname.drop_duplicates(subset=['sku'])
    
    # lấy dữ liệu sku và upc từ bảng sku_upc
    cursor.execute("SELECT * FROM sku_upc")
    rows = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    df_sku_upc = pd.DataFrame(rows, columns=columns)
    df_sku_upc = df_sku_upc.drop_duplicates(subset=['sku'])

    
    region = ['au', 'ca', 'de', 'es', 'fr', 'it', 'nl', 'uk', 'us']
    list_table = ['awe_amazon_date_range_report_' + i for i in region]
    
    # lấy tất cả dữ liệu đích danh
    for table in list_table:
        cursor.execute(f"SELECT * FROM {table} WHERE {table}.sku IS NOT NULL")
        rows = cursor.fetchall()
        # lấy tên cột
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        

        df_dich_danh = df
        df_dich_danh['product_name'] = df_dich_danh['sku'].map(df_sku_productname.set_index('sku')['product_name'])
        df_dich_danh['upc'] = df_dich_danh['sku'].map(df_sku_upc.set_index('sku')['upc'])
        
        
        # tạo bảng đích danh
        cursor.execute(f"DROP TABLE IF EXISTS {table}_productname_upc")
        conn.commit()
        cursor.execute(f"CREATE TABLE {table}_productname_upc ( {', '.join([f'{col} TEXT NULL' for col in df_dich_danh.columns])})")
        conn.commit()
        # Chuẩn bị dữ liệu cho chèn nhiều hàng
        processed_data = []
        df_dich_danh = df_dich_danh.replace({pd.NA: None, pd.NaT: None, np.nan: None})
        for index, row in df_dich_danh.iterrows():
            processed_data.append(tuple(row[col] for col in df_dich_danh.columns))
        
        # Câu lệnh SQL với placeholder
        sql = f"INSERT INTO {table}_productname_upc ({', '.join([f'{col}' for col in df_dich_danh.columns])}) VALUES ({', '.join(['%s' for col in df_dich_danh.columns])})"
        # Thực thi câu lệnh SQL
        cursor.executemany(sql, processed_data)
        conn.commit()
            
    # lấy dữ liệu không đích danh
    for table in list_table:
        cursor.execute(f"SELECT * FROM {table} WHERE {table}.sku IS NULL")
        rows = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        
        cursor.execute(f"DROP TABLE IF EXISTS {table}_total")
        conn.commit()
        cursor.execute(f"CREATE TABLE {table}_total ( {', '.join([f'{col} TEXT NULL' for col in df.columns])})")
        conn.commit()
        processed_data = []
        df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})
        for index, row in df.iterrows():
            processed_data.append(tuple(row[col] for col in df.columns))
        
        sql = f"INSERT INTO {table}_total ({', '.join([f'{col}' for col in df.columns])}) VALUES ({', '.join(['%s' for col in df.columns])})"
        cursor.executemany(sql, processed_data)
        conn.commit()
    cursor.close()
    conn.close()
    
    
        
        
    

        
        



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
    'transform_data_report',
    default_args=default_args,
    description='Scan recent file in s3',
    schedule_interval='@daily',
    catchup=False,
    tags= ['amazon']
) as dag:
    get_data_date_range_report = PythonOperator(
        task_id='get_data_date_range_report',
        python_callable=get_data_date_range_report
    )
    
    get_data_date_range_report  