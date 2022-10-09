from modulos_principais.realiza_webscraping import realiza_webscraping
from modulos_principais.insere_dados_postgres import insere_categorias
from modulos_principais.insere_dados_postgres import insere_produtos


if __name__ == "__main__":
	realiza_webscraping()
	insere_categorias()
	insere_produtos()