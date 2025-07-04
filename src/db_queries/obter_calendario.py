from src.db_connectors import mysql_lake_comercial
from src.config import TABLE_UPDATE_INTERVAL,YESTERDAY
import pandas as pd

def obter_calendario_semanal()-> pd.DataFrame:

    db_lake_comercial = mysql_lake_comercial.DatabaseConnector('analytics_performance')
        
    df_calendario = pd.read_sql_query("SELECT * FROM analytics_performance.vw_dim_calendario_intervalo_semanas", db_lake_comercial.create_engine(), dtype={'date':'datetime64[ns]'})

    return df_calendario

def obter_calendario_dias_uteis()-> pd.DataFrame:

    db_lake_comercial = mysql_lake_comercial.DatabaseConnector('analytics_performance')


    query=f'''
    SELECT date,work_day 
    FROM analytics_performance.vw_dim_calendario
    WHERE DATE(date) >= DATE_FORMAT(DATE_SUB('{YESTERDAY}', INTERVAL {TABLE_UPDATE_INTERVAL} MONTH), '%%Y-%%m-01');
    '''
        
    df_calendario = pd.read_sql_query(query, db_lake_comercial.create_engine(), dtype={'date':'datetime64[ns]'})

    return df_calendario