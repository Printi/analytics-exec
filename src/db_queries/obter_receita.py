from src.db_connectors import mysql_lake_comercial
import pandas as pd
from src.config import TABLE_UPDATE_INTERVAL,YESTERDAY

def obter_receita_diaria_ajustada_vendedor()-> pd.DataFrame:
    """
    Descrição:
        Função para obter a receita diária por vendedor.
    Parametros:
        YESTERDAY (str): Data de referência no formato 'YYYY-MM-DD'.
        TABLE_UPDATE_INTERVAL: Intervalo em meses para atualização.
    Retorno:
        pd.DataFrame: DataFrame contendo a receita diária.
    """

    db_lake_comercial = mysql_lake_comercial.DatabaseConnector('analytics_performance')


    query_receita = f"""
        SELECT 
            DATE(p.order_invoice_date) AS 'invoice_date',
            UPPER(p.nome_equipe_chave_invoice) AS team,
            email_vendedor AS vendedor,
            COUNT(DISTINCT p.id_order) AS quantity_order,
            COUNT(DISTINCT cg.GrupoID_final) AS quantity_clients,
            ROUND(SUM(p.total_price_order),2) AS receita
        FROM 
            analytics_performance.gold_pedidos_comercial_dashboard p
            INNER JOIN analytics_performance.silver_customer_groupID cg ON p.customer_id = cg.customer_id
        WHERE
            DATE(p.order_invoice_date) >= DATE_FORMAT(DATE_SUB('{YESTERDAY}', INTERVAL {TABLE_UPDATE_INTERVAL} MONTH), '%%Y-%%m-01')
            AND DATE(p.order_invoice_date) <= '{YESTERDAY}'
            AND required_invoice = 1
        GROUP BY
            1,2,3
        ORDER BY
            1,2,3;
    """

    local_df_receita = pd.read_sql_query(query_receita, db_lake_comercial.create_engine(), dtype={'invoice_date': 'datetime64[ns]', 'team': 'category', 'vendedor':'category'})

    return local_df_receita

