
from bs4 import BeautifulSoup


def obter_dados_produto (element):
    resultado = None
    if ("star-rating" in element["class"]):
        classe = element["class"]
        if ("One" in classe):
            qtd_estrelas = 1
        elif ("Two" in classe):
            qtd_estrelas = 2
        elif ("Three" in classe):
            qtd_estrelas = 3
        elif ("Four" in classe):
            qtd_estrelas = 4
        elif ("Five" in classe):
            qtd_estrelas = 5
        resultado = qtd_estrelas
    elif ("price_color" in element["class"]):
        resultado = float(element.text[1:])
    else:
        resultado = 1 if element.text.strip() == "In stock" else 0
    return resultado