import pymysql
import pandas as pd
import warnings
from dotenv import load_dotenv
import os
import time
from src.utils.func import print_log
from src.utils.slack import enviar_msg_slack_canal

load_dotenv()
warnings.filterwarnings("ignore")  # Ignora avisos que não sejam erros

def simple_connection():
    try:
        conexao = pymysql.connect(
            host=os.getenv('HOST_RELACIONAMENTO'),
            user=os.getenv('USER_RELACIONAMENTO'),
            password=os.getenv('PASSWD_RELACIONAMENTO'),
            cursorclass=pymysql.cursors.DictCursor  # Retorna os resultados como dicionário
        )
        print_log("MySQL BD RELACIONAMENTO >>> Conexão bem-sucedida!")
        return conexao

    except pymysql.MySQLError as e:
        print_log(f'\nMySQL BD RELACIONAMENTO >>> Falha na conexão: {e}\n')

