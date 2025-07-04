from src.db_connectors import mysql_lake_comercial
import pandas as pd
from src.config import TABLE_UPDATE_INTERVAL,YESTERDAY

def obter_pedidos_diario_vendedor()-> pd.DataFrame:
    """
    Descrição:
        Função para obter os pedidos diários  por vendedor.
    Parametros:
        YESTERDAY (str): Data de referência no formato 'YYYY-MM-DD'.
        TABLE_UPDATE_INTERVAL: Intervalo em meses para atualização.
    Retorno:
        pd.DataFrame: DataFrame contendo os pedidos diários.
    """

    db_lake_comercial = mysql_lake_comercial.DatabaseConnector('analytics_performance')
        
    

    query = f"""
        SELECT
            DATE(created_at_order) AS created_order_date
            ,p.nome_equipe_chave_pedido AS team
            ,p.email_vendedor AS vendedor
            ,COUNT(DISTINCT p.id_order) AS quantity_order
            ,COUNT(DISTINCT CASE WHEN p.order_origin = 'QUOTATION' THEN p.id_order ELSE NULL END) AS quantity_order_with_quotation
            ,COUNT(DISTINCT cg.GrupoID_final) AS quantity_clients
            ,ROUND(SUM(p.total_price_order),2) AS receita
        FROM 
            analytics_performance.gold_pedidos_comercial_dashboard p
            INNER JOIN analytics_performance.silver_customer_groupID cg ON p.customer_id = cg.customer_id
        WHERE
            DATE(p.created_at_order) >= DATE_FORMAT(DATE_SUB('{YESTERDAY}', INTERVAL {TABLE_UPDATE_INTERVAL} MONTH), '%%Y-%%m-01')
            AND DATE(p.created_at_order) <= '{YESTERDAY}'
        GROUP BY
            1,2,3
        ORDER BY
            1,2,3;
    """

    df = pd.read_sql_query(query, db_lake_comercial.create_engine(), dtype=None)

    return df
