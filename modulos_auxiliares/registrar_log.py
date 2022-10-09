import pendulum
import traceback
import os.path
import os


def registrar_log (texto):
    path = os.getcwd()
    path = path.replace("\\","/")
    path = path  if os.path.isdir("logs") else path + "/airflow/dags"
    agora = pendulum.now()
    data_arquivo = agora.format('YYYYMMDD')
    data_hora_log = agora.format('YYYY-MM-DD HH:mm:ss')
    nome_arquivo =  path + "/logs/" + data_arquivo + "-log_webscrapping.txt"
    texto_log = ''
    try:
        if os.path.isfile(nome_arquivo):
            arquivo = open(nome_arquivo, "a")
            texto_log = texto_log + '\n'
        else:
            arquivo = open(nome_arquivo, "w")
    except:
        print("Erro na tentativa de acessar o arquivo para criação de logs")
        raise Exception(traceback.format_exc())
    
    texto_log = texto_log + "[" + data_hora_log + "] - " + texto
    arquivo.write(texto_log)
    arquivo.close()