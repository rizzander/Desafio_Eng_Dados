from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta  
import json
import requests
import pandas as pd
import os

output_folder = './dags'

def get_data_and_group_by_year(endpoint, output_folder):
    data_by_year = {}
    page = 1
    while True:
        response = requests.get(endpoint, params={'page': page})
        if response.status_code == 200:
            data = response.json()
            for result in data['results']:
                created_year = result['created'][:4]
                if created_year not in data_by_year:
                    data_by_year[created_year] = []
                data_by_year[created_year].append(result)
            if data['next']:
                page += 1
            else:
                break
        else:
            print(f"Falha ao obter dados de {endpoint}")
            return

    for year, results in data_by_year.items():
        filename = f"{output_folder}/{endpoint.split('/')[-2]}/{year}/{endpoint.split('/')[-2]}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(results, f, indent=4)

def count_records(people_endpoint, output_folder):
    url = people_endpoint
    num_records = 0

    while url:
        response = requests.get(url)
        data = response.json()
        num_records += len(data['results'])
        url = data['next']
    os.makedirs(output_folder, exist_ok=True)
    with open(f"{output_folder}/contagem.txt", 'w') as f:
        f.write(f"O número total de registros do endpoint 'people' é: {num_records}")
    return num_records

def transform(people_endpoint, films_endpoint, output_folder):
    people_endpoint = 'https://swapi.dev/api/people/'
    films_endpoint = 'https://swapi.dev/api/films/'
    response_people = requests.get(people_endpoint)
    people_data = response_people.json()

    response_films = requests.get(films_endpoint)
    films_data = response_films.json()

    transformed_data = []
    for person_data in people_data['results']:
        transformed_person = {
            'name': person_data['name'],
            'height': person_data['height'],  
            'gender': person_data['gender'],
            'films': []  
        }

        for film_url in person_data['films']:
            response_film = requests.get(film_url)
            film_data = response_film.json()
            transformed_person['films'].append(film_data['title'])

        transformed_data.append(transformed_person)

    transformed_films = []
    for film_data in films_data['results']:
        transformed_film = {
            'title': film_data['title']
        }
        transformed_films.append(transformed_film)

    final_json = {
        'people': transformed_data,
        'films': transformed_films
    }
    os.makedirs(output_folder, exist_ok=True)
    # Salvar os dados transformados em um arquivo JSON
    with open(f"{output_folder}/transformed_data.json", 'w') as outfile:
        json.dump(transformed_data, outfile, indent=4)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}


endpoints = {
    'people': 'https://swapi.dev/api/people/',
    'films': 'https://swapi.dev/api/films/',
    'vehicles': 'https://swapi.dev/api/vehicles/'
}


dag = DAG(
    'star_wars_etl',
    default_args=default_args,
    description='Extrai dados da API do Star Wars, conta registros e transforma os dados',
    schedule_interval=None,  # Não agendado, para execução manual
)

extract_data_people_task = PythonOperator(
    task_id='extrair_dados_people',
    python_callable=get_data_and_group_by_year,
    op_args=[endpoints['people'], output_folder],
    dag=dag,
)

extract_data_films_task = PythonOperator(
    task_id='extrair_dados_films',
    python_callable=get_data_and_group_by_year,
    op_args=[endpoints['films'], output_folder],
    dag=dag,
)

extract_data_vehicles_task = PythonOperator(
    task_id='extrair_dados_vehicles',
    python_callable=get_data_and_group_by_year,
    op_args=[endpoints['vehicles'], output_folder],
    dag=dag,
)

count_records_task = PythonOperator(
    task_id='contar_registros',
    python_callable=count_records,
    provide_context=True,
    op_args=[endpoints['people'], output_folder], 
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transformar',
    python_callable=transform,
    provide_context=True,
    op_args=[endpoints['people'], endpoints['films'], output_folder],
    dag=dag,
)

extract_data_people_task >> extract_data_films_task >> extract_data_vehicles_task >> count_records_task >> transform_task

