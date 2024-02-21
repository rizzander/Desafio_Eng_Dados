# Desafio - Data Engineer
 A equipe de Produtos quer lançar um novo filme de ficção científica. Para isso, eles precisam de dados dos filmes do Star Wars. Felizmente, os dados estão disponíveis nessa API https://swapi.dev/. Para isso, será necessário extrair dados de alguns endpoints da API e internalizar em um destino. 
1. Desenvolva um script que quando executado extraia os dados da API do Star Wars no endpoint people, films e vehicles e armazene o retorno da API de todas as paginações localmente em arquivos no formato .json. Salve os arquivos baseado em uma estrutura de diretórios de acordo com o ano contido no conteúdo da propriedade created. 
Ex de estrutura de diretório:
•    Peoples/{year}/*.json
•    Films/{year}/*.json
•    Vehicles/{year}/*.json
2. Desenvolva transformações que respondam as seguintes questões. 
•    Quantos registros foram retornados do endpoint people e salvo nos arquivos?
• No retorno das requisições do endpoint de https://swapi.dev/api/people, temos uma lista dos filmes vinculados com cada registro de people. Desenvolva um script que salve em um arquivo .json o nome da pessoa e uma lista de títulos dos filmes vinculados aos registros de people.

## Objetivos:

Demonstrar a habilidades de orquestrar um ETL utilizando o Airflow. 

## Solução Proposta:
Utilizar o Airflow para orquestrar toda extração e transformação solicitada, e obter assim os arquivos solicitados e a informação do numero de registros na tela. Assim como desenvolver uma aplicação que faça a mesma coisa localmente.
 
## Instruções de uso:

 - Clone este repositório localmente.
 - Abra o diretório criado e execute no seu editor e o comando: docker-compose up.
 - Abra a porta localhost:8080 e abra o Airflow, em seguida localize a DAG "star_wars_etl" e execute.
  - Os arquivos estarão no diretório de execução da DAG assim como o arquivo de texto com a contagem de registros em people
  - Para executar localmente, basta rodar o programa extract.py que ele fara a mesma coisa, com a diferença que o número de contagem de registros aparecerá como um print no console python.

## Considerações finais:

Este está sendo meu primeiro contato prático com o Airflow, tive s oportunidade de aprender seus conceitos basicos e assim poder adiquirir mais uma nova habilidade

OBRIGADO!

