# Import
import psycopg
import traceback
import os
from dotenv import load_dotenv, find_dotenv
from modulos_auxiliares.registrar_log import registrar_log

load_dotenv(find_dotenv())

DBNAME = os.environ.get("DBNAME")
DBPORT = os.environ.get("DBPORT")
DBUSER = os.environ.get("DBUSER")
DBPASSWORD = os.environ.get("DBPASSWORD")
DBHOST = os.environ.get("DBHOST")

# Cria conexão
registrar_log("Conectando ao SGBD Postgres...")
try:
	conn = psycopg.connect(dbname = DBNAME, port = DBPORT, user = DBUSER, password = DBPASSWORD, host = DBHOST)
except:
	registrar_log("Erro ao tentar se conectar ao banco de dados")
	raise Exception(traceback.format_exc())

try:
	# Abre um cursor
	registrar_log("Conexão feita com sucesso...")
	cursor = conn.cursor()

	# Cria as tabelas de dimensão
	registrar_log("Criando tabelas de dimensão...")

	# Dimensão com range de preço (pricerange) e posição no ranking (rankingposition)
	cursor.execute("CREATE TABLE IF NOT EXISTS tb_categorias(id INT PRIMARY KEY NOT NULL , categoria VARCHAR (30))")

	# Dimensão com prêmios (awards) e avaliação de usuário (rating)
	cursor.execute("CREATE TABLE IF NOT EXISTS tb_produtos(id SERIAL PRIMARY KEY NOT NULL, nome VARCHAR (100), id_categoria INT, preco REAL, em_estoque INT, estrelas INT, CONSTRAINT fk_categoria FOREIGN KEY (id_categoria) REFERENCES tb_categorias(id) )")
	conn.commit()

	registrar_log("Estrutura do DW criada com sucesso!")
except:
	registrar_log("Erro ao tentar criar as tabelas no banco de dados")
	raise Exception(traceback.format_exc())