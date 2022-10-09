# Webscraping e ingestão de dados no PostgreSQL
## Introdução
Realização de Webscraping no site http://books.toscrape.com/. Os dados são salvos em .csv e depois no PostgreSQL. O Airflow agenda a execução das atividades.

**Tecnologias Usadas**: Python, Docker, PostgreSQL, Airflow

## Preparação e Execução do Projeto

### Testes

Instale todas as dependências do projeto.

`pip install -r requirements.txt`

Crie as pastas.

`mkdir logs stage`

Instale o container Docker do PostgreSQL. O script de criação se encontra na pasta Docker. Na pasta conecta_postgres, crie o arquivo .env e adicione os valores necessários. Além disso, crie o banco de dados para gravar os dados do webscraping. Os comandos estão na pasta SQL. 

Instale também o Google Chrome. O arquivo chromedriver deve estar de acordo com o sistema operacional usado.

Execute o projeto.

`python test.py`

### Execução do Projeto no Airflow

Instale o Airflow. Os comandos a seguir foram pegos da documentação: 

<code>
export AIRFLOW_HOME=~/airflow

AIRFLOW_VERSION=2.4.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

airflow standalone
</code>

Passe os arquivos para a pasta dags do Airflow.

<code>
cp -R modulos_auxiliares ~/airflow/dags/ <br>
cp -R modulos_principais ~/airflow/dags/ <br>
cp -R conecta_postgres ~/airflow/dags/ <br>
mkdir ~/airflow/dags/logs ~/airflow/dags/stage <br>
cp webscraping.py ~/airflow/dags/ <br>
cp chromedriver ~/airflow/dags/
</code>

Crie o banco de dados para gravar os metadados do Airflow, os comandos estão na pasta de SQL. No arquivo ~/airflow/airflow.cfg, mude o seguinte parâmetro:

`sql_alchemy_conn = postgres://airflow:airflow123@localhost:5432/AIRFLOW` 

Instale todas as dependências do projeto.

`pip install -r requirements.txt`

Instale o container Docker do PostgreSQL. O script de criação se encontra na pasta Docker. Na pasta conecta_postgres, crie o arquivo .env e adicione os valores necessários. Além disso, crie o banco de dados para gravar os dados do webscraping. Os comandos estão na pasta SQL. 

Instale também o Google Chrome. O arquivo chromedriver deve estar de acordo com o sistema operacional usado.

Inicialize o banco de dados do Airflow.

`airflow db init`

Inicialize o scheduler do Airflow.

`airflow scheduler`

Crie o usuário para o Airflow. Exemplo:

<code>
airflow users create \ <br>
    --username admin \ <br>
    --password admin123 <br>
    --firstname Peter \ <br>
    --lastname Parker \ <br>
    --role Admin \ <br>
    --email spiderman@superhero.org
</code>

Inicialize o servidor Web.

`airflow webserver`

Execute a Dag.

<img src="./Tela_Airflow.PNG"></img>