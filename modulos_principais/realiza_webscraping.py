from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import numpy as np
import pendulum
import os
import os.path
import traceback
from modulos_auxiliares.registrar_log import registrar_log
from modulos_auxiliares.obter_dados_produto import obter_dados_produto


def obtem_dados(driver):
	registrar_log("Buscando as categorias")
	pagina_completa_bs4 = BeautifulSoup(driver.page_source, 'html.parser')
	side_categories_html = pagina_completa_bs4.find(class_ = 'side_categories')
	side_categories_bs4 = BeautifulSoup(str(side_categories_html), 'html.parser')
	side_categories_text = [a.text.strip() for a in side_categories_bs4.find_all('a')[1:]]

	registrar_log("Foram encontradas " + str(len(side_categories_text)) + " categorias")

	df_categorias = pd.DataFrame(columns=['Id','Descricao'])
	df_categorias = df_categorias.astype(dtype = {'Id': "int64", 'Descricao': "str"})

	df_produtos = pd.DataFrame(columns=['Id_Categoria','Nome_Produto', "Preco",'Estrelas', 'Em_Estoque'])
	df_produtos = df_produtos.astype(dtype = {'Id_Categoria': "int64", 'Nome_Produto': "str", "Preco": "float",
	                                          "Estrelas": "int64", "Em_Estoque": "int64"})

	for i in range(0,len(side_categories_text)):
	    registrar_log("Pecorrendo categoria "+str(i+1))
	    category_text = side_categories_text[i]
	    id_category = i + 1
	    registrar_log("Categoria "+category_text)
	    
	    df_temp = pd.DataFrame(data = {'Id': [id_category], 'Descricao': [category_text]})
	    df_categorias = pd.concat([df_categorias,df_temp] )
	    
	    side_categories = driver.find_elements(By.CLASS_NAME, "side_categories")
	    side_category = side_categories[0].find_element(By.LINK_TEXT, side_categories_text[i])

	    side_category.click()
	    
	    pagina_completa_bs4 = BeautifulSoup(driver.page_source, features= 'html.parser')
	    element_next= pagina_completa_bs4.find(class_ = 'next')
	    element_current = pagina_completa_bs4.find(class_ = "current")
	    last_page = 1

	    if element_next != None:
	        element_current_text = element_current.text.strip()
	        last_and_actual_page = element_current_text.split(" of ")
	        last_page = int(last_and_actual_page[1])
	         
	    registrar_log("Número de páginas da categoria "+category_text+ ": " + str(last_page) )   
	    
	    for page in range(1, (last_page + 1)):
	        registrar_log("Verificando produtos da página "+str(page)+ " da categoria " + category_text )
	        
	        element_products_category_list = driver.find_elements(By.CLASS_NAME, "product_pod")
	        produtos_html = pagina_completa_bs4.find_all(class_ = 'product_pod')
	        for product_element in produtos_html:
	            produto_bs4 = BeautifulSoup(str(product_element), 'html.parser')

	            nome_produto = produto_bs4.find('h3').text.strip()
	            dados_produto_html = produto_bs4.find_all('p')
	            demais_dados = [data for data in list(map(obter_dados_produto,dados_produto_html))]
	            
	            registrar_log("Verificando dados do produto "+nome_produto )
	            
	            dados_produto = {'Id_Categoria': [id_category], 'Nome_Produto': [nome_produto], "Preco": [demais_dados[1]],
	                             "Estrelas": [demais_dados[0]], "Em_Estoque": [demais_dados[2]]}
	            
	            texto_dados_produtos = 'Id_Categoria: %s, Nome_Produto: %s, Preço: %s, Estrelas: %s, Em_Estoque: %s' % (str(id_category),
	                                                                                                                       str(nome_produto),
	                                                                                                                       str(demais_dados[1]),
	                                                                                                                       str(demais_dados[0]),
	                                                                                                                       str(demais_dados[2]))
	            registrar_log(texto_dados_produtos )
	            registrar_log("" )
	            df_temp = pd.DataFrame(data = dados_produto)
	            df_produtos = pd.concat( [df_produtos,df_temp] )
	        
	        pagina_completa_bs4 = BeautifulSoup(driver.page_source, 'html.parser')
	        element_next_tag_bs4 = pagina_completa_bs4.find(class_ = 'next')
	        if element_next_tag_bs4 != None:
	            element_next= driver.find_element(By.CLASS_NAME, "next")
	            driver.implicitly_wait(15)
	            ActionChains(driver).move_to_element(element_next).click(element_next).perform()
	    registrar_log("" )
	    registrar_log("----------------------------------------------------------------------------------------------------------" )

	registrar_log("WebScrapping Realizado!" )
	driver.close()
	return df_categorias, df_produtos

def realiza_webscraping ():
	path = os.getcwd()
	path = path.replace("\\","/")
	path = path  if os.path.isdir("logs") else path + "/airflow/dags"

	try:
		ser = Service(path + "/chromedriver")
		op = webdriver.ChromeOptions()
		op.add_argument('--headless')
		driver = webdriver.Chrome(service=ser, options=op)
	except:
		registrar_log("Erro na tentativa de acessar o chrome driver")
		raise Exception(traceback.format_exc())

	try:
		driver.get("http://books.toscrape.com/")
		driver.implicitly_wait(15)
	except:
		registrar_log("Erro na tentativa de acessar a url http://books.toscrape.com/")
		raise Exception(traceback.format_exc())

	try:
		df_categorias, df_produtos = obtem_dados(driver)
	except:
		registrar_log("Erro na tentativa de ler os dados")
		raise Exception(traceback.format_exc())

	registrar_log("Salva dados em arquivos na pasta stage")
	try:
		df_categorias.to_csv( path + "/stage/categorias.csv", index = False)
		df_produtos.to_csv(path + "/stage/produtos.csv", index = False)	
		registrar_log("Salvamento realizado" )
	except:
		registrar_log("Erro na tentativa de salvar os dados na pasta stage")
		raise Exception(traceback.format_exc())
