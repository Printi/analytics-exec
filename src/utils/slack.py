from slack_sdk import WebClient
from datetime import datetime
from dotenv import load_dotenv
import os
import pytz


def enviar_msg_slack_canal(texto_msg, canal="C08TLPQQDT8"):
    """Enviar mensagens para um canal"""    
    client = WebClient(os.getenv("SLACK_BOT_TOKEN"))
    datetime_brasil = str(datetime.now(tz=pytz.timezone('America/Sao_Paulo')))
    mensagem = client.chat_postMessage(channel=canal,text=f"{datetime_brasil[0:19]} ::: {texto_msg}")
    info=f"{datetime_brasil[0:19]} ::: {texto_msg}"
    
    return info

