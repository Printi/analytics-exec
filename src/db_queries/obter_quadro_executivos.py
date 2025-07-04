from src.db_connectors import mysql_lake_comercial
import pandas as pd
from src.config import TABLE_UPDATE_INTERVAL,YESTERDAY

def obter_quadro_diario_colaboradores()-> pd.DataFrame:
    """
    Descrição:
        Função para obter o quadro dos colaboradores.
    Parametros:
        YESTERDAY (str): Data de referência no formato 'YYYY-MM-DD'.
        TABLE_UPDATE_INTERVAL: Intervalo em meses para atualização.
    Retorno:
        pd.DataFrame: DataFrame contendo o quadro.
    """

    db_lake_comercial = mysql_lake_comercial.DatabaseConnector('analytics_performance')
        
    query = f"""
        SELECT
            DATE(q.data_escala) AS data,
            q.email_agente AS vendedor
        FROM 
            analytics_performance.tb_quadro_colaboradores as q
        WHERE
            DATE(q.data_escala) >= DATE_FORMAT(DATE_SUB('{YESTERDAY}', INTERVAL {TABLE_UPDATE_INTERVAL} MONTH), '%%Y-%%m-01')
            AND DATE(q.data_escala) <= '{YESTERDAY}';
    """

    df = pd.read_sql_query(query, db_lake_comercial.create_engine(), dtype=None)

    return df
