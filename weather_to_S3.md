# Введение

Это Airflow DAG, который: 
- проверяет доступность API погоды
- запрашивает текущие данные о погоде 
- преобразует их в удобный формат и сохраняет результат в формате CSV в облачное хранилище S3.
### Используемые инструменты:

* Python для извлечения и обработки данных 
* Yandex S3 для хранения данных 
* Apache Airflow для оркестрации
### Подготовительные шаги:

* Подключение к API openweathermap для извлечения данных о погоде (Extract).
* Преобразование данных из JSON (Transform).
* Преобразование полученных данных в формате CSV (Transform).
* Загрузка данных в S3 bucket (Load).

-----
### DAG

```python
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json
  

def transform_data(ti):
    data = ti.xcom_pull(task_ids='extract_weather_data')
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp = data["main"]["temp"]
    feels_like= data["main"]["feels_like"]
    min_temp = data["main"]["temp_min"]
    max_temp = data["main"]["temp_max"]
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature": temp,
                        "Feels Like": feels_like,
                        "Minimun Temp":min_temp,
                        "Maximum Temp": max_temp,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                  
                        }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    file_name = 'current_weather_data_rostov_' + dt_string + '.csv'

    ycs3_credentials = {"key": "****", "secret": "****", "client_kwargs": {"endpoint_url": "https://storage.yandexcloud.net"}}
    s3_path = f's3://bucket/{file_name}'
  
    df_data.to_csv(s3_path, index=False, storage_options=ycs3_credentials)
  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,5,7),
    'email': ['none@gmail.com'],
    'email_on_failure': False
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('weather_DAG',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
         

    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Rostov-on-Don&appid=***&units=metric'
    )

  
    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        method='GET',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Rostov-on-Don&appid=***&units=metric',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )


    transform_weather_data = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_data
    )

    is_weather_api_ready >> extract_weather_data >> transform_weather_data
```
