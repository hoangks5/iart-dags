from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import requests
try:
    from bs4 import BeautifulSoup
except:
    os.system('pip install beautifulsoup4')
    from bs4 import BeautifulSoup
try:
    import pandas as pd
except:
    os.system('pip install pandas')
    import pandas as pd
    
    
def get_exchange_rate(**kwargs):
    url = "https://www.vietcombank.com.vn/KHCN/Cong-cu-tien-ich/Ty-gia"

    payload = {}
    headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'cookie': 'shell#lang=en; sxa_site=VCB; vcb#lang=vi-VN; AKA_A2=A; ak_bmsc=0CC510E4B8FDAF880C5D42F4BB77DB8C~000000000000000000000000000000~YAAQXUogF82xkWCRAQAAqluSjBiz8ioybLJ2chZvhcwDaBsgxuFaomX4JsAwRO5clkdrxmCZnjYu0kXWDM8wlHkPyRrkDo9FfgL0BE5a6kBWmm5pyHj3+T/6uETA0xXZxAqXhgoENqfxetz43UEe+DuJ6u2Pmo+JyDYD4dSBIipEy5Dx2GpC1VsyBua0RPk3XNxP8iv703IEK7nFGAOb1dGvVF+DPL+LAfZ9z2Uz+IOuICncEfXCxhhpapETXnIsn3BPwAuYfjgJA7GXJnTe9TFcbPmWOO++kuwiU/sM1rOBUH9dFNq8YRZXaVntu9zb5l0jHUbzmqMSKfgTdBuC/3X/QCDctTJqVPihXXPwQvzBHrTK5OjaZbh7LosE5NuPni8N+xFHiVvKeTbkoCy8WXl9wQ==; _ga=GA1.1.468097548.1724640289; bid_fptx24jmprtv5z14rd6raberbklvums8=91071e6c-e73c-4120-be0b-dc1341374714; bx_bucket_number=84; bx_guest_ref=b1cb8799-a800-4e42-818a-ff598d84e12c; client-time-zone=7; referrer-page-type=KHCN; referrer-page-url=/vi-VN/Error-404; _ga_VHCGJJH9VF=GS1.1.1724640289.1.1.1724640335.0.0.0; data-client-ip=118.70.190.129, 23.32.74.93, 118.71.159.37; bm_sv=79EBAD8BE14282D6AE6C301A2CD7030E~YAAQXUogFwu5kWCRAQAAQheTjBh+BtixNbDNvhr+2f582IqWJTqXTerBs1GYFgvZmkuIPZxlxh2MGUPryq3l7btAz7ERUVs7sJmyVXsAG4xwi7bc15KUs8LE93uX+g/9WQFTSNPiRU26WVmgyqwPg3zyMmqya1eFGYIvslFkNjwFbgHYayzxXxQE1VGHRfSkhme7/sECfyJYyUep1B8KLgaEOBZ3fumCVe2P5m6wpIxKX9JrqqMr2FtWpCFqJndoItgoCOEb4Tw=~1; bm_sv=79EBAD8BE14282D6AE6C301A2CD7030E~YAAQTkogFw55IoCRAQAA2/yUjBgPkdzw8ehAAe6QpwOHx7Kz4MbXFcC95vJxxQZDUJ5qDDJWFQ92viAcAzmEGRry/1cJGJrS4GYnxd2C6MrrtVz9nSIuIBXG28LWedxa0donG5YBuZunA5/lrXvARqTBT+SLz5hmCWwrYnnSSxhk2zs11pOV+tF+PZLOhFb9/7eMl+dK2WS0MWWnqBOckXjmuMe+loaP009y8dKM+c+2/ss9DTADNzn3bbgqkmenY54QhHUrowg=~1; data-client-ip=118.70.190.129, 23.32.74.78, 118.71.159.39; sxa_site=VCB',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version-list': '"Not)A;Brand";v="99.0.0.0", "Google Chrome";v="127.0.6533.122", "Chromium";v="127.0.6533.122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"10.0.0"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, 'html.parser')
    tbody = soup.find_all(class_='dropdown-options__item')
    df = pd.DataFrame(columns=['data_code', 'data_cash_rate', 'data_transfer_rate', 'data_sell_rate'])
    for i in tbody:
        data_code = i.get('data-code')
        data_cash_rate = i.get('data-cash-rate')
        data_transfer_rate = i.get('data-transfer-rate')
        data_sell_rate = i.get('data-sell-rate')
        df = pd.concat([df, pd.DataFrame({'data_code': [data_code], 'data_cash_rate': [data_cash_rate], 'data_transfer_rate': [data_transfer_rate], 'data_sell_rate': [data_sell_rate]})])
        
    
    # lưu vào xcom
    kwargs['ti'].xcom_push(key='exchange_rate', value=df)
    
    
def write_to_db(**kwargs):
    df = kwargs['ti'].xcom_pull(key='exchange_rate', task_ids='get_exchange_rate')
    # lưu vào db
    print(df)
    
    
    
    
    
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
    'get_exchange_rate',
    default_args=default_args,
    description='Lấy tỷ giá ngoại tệ từ trang web của Vietcombank',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags= ['amazon']
) as dag:
    get_exchange_rate = PythonOperator(
        task_id='get_exchange_rate',
        python_callable=get_exchange_rate
    )

    write_to_db = PythonOperator(
        task_id='write_to_db',
        python_callable=write_to_db
    )
    
    
    
get_exchange_rate >> write_to_db