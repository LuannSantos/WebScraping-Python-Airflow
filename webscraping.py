from airflow.models import DAG
from airflow.operators.python import PythonOperator
import pendulum
from modulos_principais.realiza_webscraping import realiza_webscraping
from modulos_principais.insere_dados_postgres import insere_categorias
from modulos_principais.insere_dados_postgres import insere_produtos
#Dag    
with DAG(dag_id = 'DAG_WEBSCRAPING', start_date = pendulum.datetime(2022, 1, 1, tz = "UTC"), schedule_interval = '@Daily', catchup = False) as dag:
	task1_realiza_webscraping = PythonOperator(
		task_id = 'realiza_webscraping',
		python_callable = realiza_webscraping,
		dag = dag
	)

	# Tarefa de extração de atributos
	task2_insere_categorias = PythonOperator(
		task_id = 'insere_categorias',
		python_callable = insere_categorias,
		dag = dag
	)

	# Tarefa de extração do range de preço
	task3_insere_produtos = PythonOperator(
		task_id = 'insere_produtos',
		python_callable = insere_produtos,
		dag = dag
	)

task1_realiza_webscraping >> task2_insere_categorias >> task3_insere_produtos
	

    
    
    
        
        


