from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta  
import json
import requests
import pandas as pd

# Define a função para obter todos os dados de um endpoint da API e agrupar por ano de criação
def get_all_data_and_group_by_year(endpoint, directory):
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
        filename = f"{directory}/{year}/{endpoint.split('/')[-2]}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(results, f, indent=4)

# Define a função para contar o número de registros do endpoint 'people'
def count_records(**kwargs):
    ti = kwargs['ti']  # Obtém o objeto TaskInstance
    people_data = ti.xcom_pull(task_ids='extrair_dados')  # Obtém os dados da tarefa extrair_dados
    num_records = sum(len(data) for data in people_data.values())  # Conta o número total de registros
    print(f"O número total de registros do endpoint 'people' é: {num_records}")

# Define a função para extrair os títulos dos filmes vinculados a cada pessoa e salvá-los em um arquivo JSON
def transform(**kwargs):
    ti = kwargs['ti']  # Obtém o objeto TaskInstance
    people_data = ti.xcom_pull(task_ids='extrair_dados')  # Obtém os dados da tarefa extrair_dados
    
    # Carrega os dados de filmes
    films_data = pd.read_json(people_data['people'])  # Supondo que a saída da tarefa extrair_dados foi salva com a chave 'people'
    
    def extract_films_titles(films_urls):
        films_titles = []
        for film_url in films_urls:
            film_id = film_url.split('/')[-2]
            film_title = films_data[films_data['url'].str.contains(film_id)]['title'].iloc[0]
            films_titles.append(film_title)
        return films_titles

    people_films = []
    for year_data in people_data.values():
        for person_data in year_data:
            films_titles = extract_films_titles(person_data['films'])
            people_films.append({
                'name': person_data['name'],
                'gender': person_data['gender'],
                'films': films_titles
            })

    with open('people_with_films.json', 'w') as outfile:
        json.dump(people_films, outfile, indent=4)

# Definir argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

# Definir os endpoints da API
endpoints = {
    'people': 'https://swapi.dev/api/people/',
    'films': 'https://swapi.dev/api/films/',
    'vehicles': 'https://swapi.dev/api/vehicles/'
}

# Definir a DAG
dag = DAG(
    'star_wars_etl',
    default_args=default_args,
    description='Extrai dados da API do Star Wars, conta registros e transforma os dados',
    schedule_interval=None,  # Não agendado, para execução manual
)

# Definir as tasks da DAG
extract_data_task = PythonOperator(
    task_id='extrair_dados',
    python_callable=get_all_data_and_group_by_year,
    op_args=[endpoints['people'], 'people'],
    dag=dag,
)

count_records_task = PythonOperator(
    task_id='contar_registros',
    python_callable=count_records,
    provide_context=True, 
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transformar',
    python_callable=transform,
    provide_context=True,  
    dag=dag,
)

# Definir as dependências entre as tasks
extract_data_task >> count_records_task >> transform_task
