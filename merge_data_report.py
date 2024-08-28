from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from airflow.models import Variable
from datetime import datetime, timezone
import re
from dateutil import parser
import hashlib
try:
    import numpy as np
except:
    os.system('pip install numpy')
    import numpy as np
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
    
    
def convert_str_to_number(string):
    cv_str = str(string).replace(',','.')
    # nếu trong cv_str có nhiều hơn 1 dấu chấm thì xóa hết dấu chấm giữ lại 1 dấu đầu tiên từ phải qua trái
    if cv_str.count('.') > 1:
        print('cv_str ban đầu:', cv_str)
        cv_str = cv_str.replace('.','',cv_str.count('.')-1)
        print('cv_str sau khi xử lý:', cv_str)
    return float(cv_str)

def convert_data(df, region):
    date_time_dict = df.to_dict(orient='list')
    if date_time_dict == {}:
        return df
    if region == 'au':
        for index, date in enumerate(date_time_dict['date/time']):
            date_new = parser.parse(date)
            date_time_dict['date/time'][index] = date_new
        key_chekc_isnumric = ['settlement id','order postal','product sales','shipping credits','gift wrap credits','promotional rebates','sales tax collected','low value goods','selling fees','fba fees','other transaction fees','other','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
    
    
    elif region == 'ca':
        for index, date in enumerate(date_time_dict['date/time']):
            date_new = parser.parse(date)
            date_time_dict['date/time'][index] = date_new
        key_chekc_isnumric = ['settlement id','product sales','product sales tax','shipping credits','shipping credits tax','gift wrap credits','giftwrap credits tax','Regulatory fee','Tax on regulatory fee','promotional rebates','promotional rebates tax','marketplace withheld tax','selling fees','fba fees','other transaction fees','other','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
    
    
    elif region == 'de':
        for index, date in enumerate(date_time_dict['Datum/Uhrzeit']):
            date_new = parser.parse(date)
            date_time_dict['Datum/Uhrzeit'][index] = date_new
        key_chekc_isnumric = ['Abrechnungsnummer','Umsätze','Produktumsatzsteuer','Gutschrift für Versandkosten','Steuer auf Versandgutschrift','Gutschrift für Geschenkverpackung','Steuer auf Geschenkverpackungsgutschriften','Rabatte aus Werbeaktionen','Steuer auf Aktionsrabatte','Einbehaltene Steuer auf Marketplace','Verkaufsgebühren','Gebühren zu Versand durch Amazon','Andere Transaktionsgebühren','Andere','Gesamt']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
    
                
    elif region == 'es':
        for index, date in enumerate(date_time_dict['fecha y hora']):
            moth_translate = {
                'ene': 'Jan',
                'feb': 'Feb',
                'mar': 'Mar',
                'abr': 'Apr',
                'may': 'May',
                'jun': 'Jun',
                'jul': 'Jul',
                'ago': 'Aug',
                'sep': 'Sep',
                'oct': 'Oct',
                'nov': 'Nov',
                'dic': 'Dec'
            }
            for key in moth_translate:
                date = date.replace(key, moth_translate[key])
            date_new = parser.parse(date)
            date_time_dict['fecha y hora'][index] = date_new
        key_chekc_isnumric = ['identificador de pago','ventas de productos','impuesto de ventas de productos','abonos de envío','impuestos por abonos de envío','abonos de envoltorio para regalo','giftwrap credits tax','devoluciones promocionales','promotional rebates tax','impuesto retenido en el sitio web','tarifas de venta','tarifas de Logística de Amazon','tarifas de otras transacciones','otro','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
    
    elif region == 'fr':
        for index, date in enumerate(date_time_dict['date/heure']):
            moth_translate = {
                'janv': 'Jan',
                'févr': 'Feb',
                'mars': 'Mar',
                'avr': 'Apr',
                'mai': 'May',
                'juin': 'Jun',
                'juil': 'Jul',
                'août': 'Aug',
                'sept': 'Sep',
                'oct': 'Oct',
                'nov': 'Nov',
                'déc': 'Dec'
            }
            for key in moth_translate:
                date = date.replace(key, moth_translate[key])
            date_new = parser.parse(date)
        key_chekc_isnumric = ['numéro de versement','ventes de produits','Taxes sur la vente des produits',"crédits d'expédition","taxe sur les crédits d’expédition","crédits sur l'emballage cadeau","Taxes sur les crédits cadeaux","Rabais promotionnels","Taxes sur les remises promotionnelles","Taxes retenues sur le site de vente","frais de vente","Frais Expédié par Amazon","autres frais de transaction","autre","total"]
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
    elif region == 'it':
        for index, date in enumerate(date_time_dict['Data/Ora:']):
            moth_translate = {
                'gen': 'Jan',
                'feb': 'Feb',
                'mar': 'Mar',
                'apr': 'Apr',
                'mag': 'May',
                'giu': 'Jun',
                'lug': 'Jul',
                'ago': 'Aug',
                'set': 'Sep',
                'ott': 'Oct',
                'nov': 'Nov',
                'dic': 'Dec'
            }
            for key in moth_translate:
                date = date.replace(key, moth_translate[key])
            date_new = parser.parse(date)
            date_time_dict['Data/Ora:'][index] = date_new
        key_chekc_isnumric = ["Numero pagamento","Vendite","imposta sulle vendite dei prodotti","Accrediti per le spedizioni","imposta accrediti per le spedizioni","Accrediti per confezioni regalo","imposta sui crediti confezione regalo","Sconti promozionali","imposta sugli sconti promozionali","trattenuta IVA del marketplace","Commissioni di vendita","Costi del servizio Logistica di Amazon","Altri costi relativi alle transazioni","Altro","totale"]
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
    
    elif region == 'jp':
        for index, date in enumerate(date_time_dict['日付/時間']):
            date_new = parser.parse(date)
        key_chekc_isnumric = ['決済番号','商品売上','商品の売上税','配送料','配送料の税金','ギフト包装手数料','ギフト包装クレジットの税金','Amazonポイントの費用','プロモーション割引額','プロモーション割引の税金','源泉徴収税を伴うマーケットプレイス','手数料','FBA 手数料','トランザクションに関するその他の手数料','その他','合計']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
        
    elif region == 'mx':
        for index, date in enumerate(date_time_dict['fecha/hora']):
            moth_translate = {
                'ene': 'Jan',
                'feb': 'Feb',
                'mar': 'Mar',
                'abr': 'Apr',
                'may': 'May',
                'jun': 'Jun',
                'jul': 'Jul',
                'ago': 'Aug',
                'sep': 'Sep',
                'oct': 'Oct',
                'nov': 'Nov',
                'dic': 'Dec'
            }
            for key in moth_translate:
                date = date.replace(key, moth_translate[key])
            date_new = parser.parse(date)
            date_time_dict['fecha/hora'][index] = date_new
        key_chekc_isnumric = ["Id. de liquidación",'ventas de productos','impuesto de ventas de productos','créditos de envío','impuesto de abono de envío','créditos por envoltorio de regalo','impuesto de créditos de envoltura','Tarifa reglamentaria','Impuesto sobre tarifa reglamentaria','descuentos promocionales','impuesto de reembolsos promocionales','impuesto de retenciones en la plataforma','tarifas de venta','tarifas fba','tarifas de otra transacción','otro','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
    
    
    elif region == 'nl':
        for index, date in enumerate(date_time_dict['datum/tijd']):
            date_new = parser.parse(date)   
            date_time_dict['datum/tijd'][index] = date_new
        key_chekc_isnumric = ['schikkings-ID','verkoop van producten','Verzendtegoeden','kredietpunten cadeauverpakking','promotiekortingen','geïnde omzetbelasting','Belasting voor marketplace-facilitator','verkoopkosten','fba-vergoedingen','overige transactiekosten','overige','totaal']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
    
    
    elif region == 'uk':
        for index, date in enumerate(date_time_dict['date/time']):
            date_new = parser.parse(date)
        key_chekc_isnumric = ['settlement id','product sales tax','postage credits','shipping credits tax','gift wrap credits','giftwrap credits tax','promotional rebates','promotional rebates tax','marketplace withheld tax','selling fees','fba fees','other transaction fees','other','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
                
    elif region == 'us':
        for index, date in enumerate(date_time_dict['date/time']):
            date_new = parser.parse(date)
            date_time_dict['date/time'][index] = date_new
        key_chekc_isnumric = ['settlement id','product sales','product sales tax','shipping credits','shipping credits tax','gift wrap credits','giftwrap credits tax','Regulatory Fee','Tax On Regulatory Fee','promotional rebates','promotional rebates tax','marketplace withheld tax','selling fees','fba fees','other transaction fees','other','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                convert_number = convert_str_to_number(value)
                date_time_dict[key][index] = convert_number
        df_new = pd.DataFrame(date_time_dict)
        return df_new
    
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
        # covert dữ liệu
        df = convert_data(df, file.split('/')[3])
        if df is None:
            print(f'Lỗi không có dữ liệu {file}')
            continue
        kwargs['ti'].xcom_push(key=file, value=df)
        
def md5_hash(row):
    concatentated = ''.join([str(row[col]) for col in row.index])
    return hashlib.md5(concatentated.encode()).hexdigest()
        
        
def transform_data_date_ranger_report(**kwargs):
    data = {}
    recent_files = kwargs['ti'].xcom_pull(task_ids='fillter_recent_files', key='recent_files')
    for file in recent_files:
        df = kwargs['ti'].xcom_pull(task_ids='read_recent_file', key=file)
        if df is None:
            print(f'Lỗi không có dữ liệu {file}')
            continue
        # đổi tên cột thành chữ thường và replace khoảng trắng và / bằng dấu _ 
        df.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('/', '_').replace("'", '_').replace(':','') for col in df.columns]
        kwargs['ti'].xcom_push(key=file, value=df)
        
        company = file.split('/')[0]
        platform = file.split('/')[1]
        account = file.split('/')[2]
        region = file.split('/')[3]
        
        
        # tạo cột hash để xác định dữ liệu mới hay cũ trong db là gộp các cột lại thành 1 chuỗi sau đó hash
        df['hash'] = df.apply(md5_hash, axis=1)
        df['account'] = account
        df['region'] = region
        df['company'] = company
        df['platform'] = platform
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
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} TEXT NULL' for col in columns])})"
        cursor.execute(query)

        # đối chiếu hash với db, nếu đã tồn tại thì xóa khỏi dataframe
        cursor.execute(f"SELECT hash FROM {table_name}")
        rows = cursor.fetchall()
        list_hash = [row[0] for row in rows]
        
        
        row_before = len(dfs[0])
        
        for index, df in enumerate(dfs):
            df = df[~df['hash'].isin(list_hash)]
            dfs[index] = df
            
        
        row_after = len(dfs[0])
        print(f'Xóa {row_before - row_after} dòng trùng lặp trong {row_before} dòng')
            
        processed_data = []
        for df in dfs:
            # Thay thế NaN bằng None để phù hợp với MySQL NULL
            df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})
            for index, row in df.iterrows():
                processed_data.append(tuple(row[col] for col in columns))
        
        # Câu lệnh SQL với placeholder
        sql = f"INSERT INTO {table_name} ({', '.join([f'{col}' for col in columns])}) VALUES ({', '.join(['%s' for col in columns])})"
        # Thực thi câu lệnh SQL
        cursor.executemany(sql, processed_data)
        
    
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
    'data_lake_to_database_raw',
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
    