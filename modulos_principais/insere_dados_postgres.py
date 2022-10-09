import pandas as pd
import numpy as np
import os
import os.path
import traceback
from modulos_auxiliares.registrar_log import registrar_log
import conecta_postgres.conecta_postgres as conn_dw


def insere_categorias ():
	path = os.getcwd()
	path = path.replace("\\","/")
	path = path  if os.path.isdir("logs") else path + "/airflow/dags"
	# Carrega o arquivo
	registrar_log("Busca dados de categorias" )
	try:
		df_categorias = pd.read_csv( path + "/stage/categorias.csv", index_col = False, delimiter = ',')
	except:
		registrar_log("Erro na tentativa de ler os dados de categorias da stage")
		raise Exception(traceback.format_exc())

	try:
		registrar_log("Desabilita todas as constraints" )
		sql = "ALTER TABLE tb_produtos DROP CONSTRAINT fk_categoria;"
		conn_dw.cursor.execute(sql)
		conn_dw.conn.commit()

		registrar_log("Limpa registros da tabela de categorias" )
		sql = "TRUNCATE TABLE tb_categorias"
		conn_dw.cursor.execute(sql)
		conn_dw.conn.commit()

		registrar_log("Insere os novos registros" )
		for i, row in df_categorias.iterrows():
		    sql = "INSERT INTO tb_categorias(id, categoria) VALUES (%s,%s)"
		    conn_dw.cursor.execute(sql, tuple(row))
		    conn_dw.conn.commit()

		registrar_log("Registros inseridos" )

		registrar_log("Reabilita as constraints" )
		sql = "ALTER TABLE tb_produtos ADD CONSTRAINT fk_categoria FOREIGN KEY (id_categoria) REFERENCES tb_categorias(id);"
		conn_dw.cursor.execute(sql)
		conn_dw.conn.commit()

		registrar_log("Inserção de dados de categorias finalizado" )
		registrar_log("" )
	except:
		registrar_log("Erro na tentativa de gravar os dados de categorias no banco de dados")
		raise Exception(traceback.format_exc())

def insere_produtos ():
	path = os.getcwd()
	path = path.replace("\\","/")
	path = path  if os.path.isdir("logs") else path + "/airflow/dags"

	registrar_log("Busca dados de produtos" )

	try:
		df_produtos = pd.read_csv( path + "/stage/produtos.csv", index_col = False, delimiter = ',')
	except:
		registrar_log("Erro na tentativa de ler os dados de produtos da stage")
		raise Execption("Erro na tentativa de ler os dados de produtos da stage")

	try:
		registrar_log("Desabilita todas as constraints" )
		sql = "ALTER TABLE tb_produtos DROP CONSTRAINT fk_categoria;"
		conn_dw.cursor.execute(sql)
		conn_dw.conn.commit()

		registrar_log("Limpa registros da tabela de produtos" )
		sql = "TRUNCATE TABLE tb_produtos"
		conn_dw.cursor.execute(sql)
		conn_dw.conn.commit()

		registrar_log("Insere os novos registros" )
		for i, row in df_produtos.iterrows():
		    sql = "INSERT INTO tb_produtos(id_categoria,nome,preco,estrelas,em_estoque) VALUES (%s,%s,%s,%s,%s)"
		    conn_dw.cursor.execute(sql, tuple(row))
		    conn_dw.conn.commit()

		registrar_log("Registros inseridos" )
		registrar_log("Reabilita as constraints" )
		sql = "ALTER TABLE tb_produtos ADD CONSTRAINT fk_categoria FOREIGN KEY (id_categoria) REFERENCES tb_categorias(id);"
		conn_dw.cursor.execute(sql)
		conn_dw.conn.commit()

		registrar_log("Inserção de dados de produtos finalizado" )
	except:
		registrar_log("Erro na tentativa de gravar os dados de produtos no banco de dados")
		raise Exception(traceback.format_exc())