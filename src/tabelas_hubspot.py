#from src.utils.func import print_log
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import os
from src.cloud_utils.mysql import mysql_query_rel
from src.utils.func import formatar_df
from datetime import datetime
from dateutil.relativedelta import relativedelta


#LIMITAR A 3 MESES DE REGISTROS - TICKETS EMAIL E WHATSAP
today= datetime.now()
first_day_of_current_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
six_months_later = first_day_of_current_month - relativedelta(months=6)

load_dotenv()
DATA_PATH=os.getenv('DATA_PATH')
archives_list=['Emails e Whats HUB.csv','Leads HUB.csv']

def get_data_hub():
    df_list=[]
    for index,archive in enumerate(archives_list):
        df_list.append(pd.read_csv(Path(DATA_PATH+archive)))

    return df_list

def merge_and_get_teams_hub(conn,list_df_data_hub:list[pd.DataFrame]):

    QUERIE_='''SELECT DISTINCT
                data AS CREATE_DATE,
                vendedor AS OWNER_EMAIL,
                channel AS CHANNEL,
                team AS TEAM
            FROM analytics_exec.fato_indicadores_diarios
            '''

    df_date_email_team=mysql_query_rel(conn,QUERIE_)
    df_date_email_team['CREATE_DATE']=df_date_email_team['CREATE_DATE'].astype(str)

    temp_list=list_df_data_hub.copy()
    for index,df in enumerate(list_df_data_hub):

        #filtragem de 150k para connector Looker Studio, limitar a 3 meses
        if index==0:
            df['CREATE_DATE']=pd.to_datetime(df['CREATE_DATE'])
            df=df[df['CREATE_DATE']>=six_months_later]


        df['CREATE_DATE']=df['CREATE_DATE'].astype(str)
        df_temp=pd.merge(df,df_date_email_team, 
                                  on=['CREATE_DATE','OWNER_EMAIL'],
                                  how='inner')
        
        #Processar ajuste no campo [Inbound] E-mail -> FARMERS também aparece INBOUND
        if 'STATUS' in list(df_temp.columns):
            df_temp['STATUS']=df_temp['STATUS'].str.replace(r'\[Inbound\]\s*', '', regex=True)

        temp_list[index]=formatar_df([df_temp])

    return temp_list

def process_data_hub(conn):
    df_list=get_data_hub()
    df_list=merge_and_get_teams_hub(conn,df_list)
    return df_list

