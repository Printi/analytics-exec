from src.db_connectors import mysql_lake_comercial
from typing import Literal
import pandas as pd
from src.config import TABLE_UPDATE_INTERVAL,YESTERDAY

def obter_ticket_valor_pago_diario_vendedor()-> pd.DataFrame:
    """
    Descrição:
        Função para obter o ticket e valor total de pedidos pagos diariamente por vendedor.
    Parametros:
        YESTERDAY (str): Data de referência no formato 'YYYY-MM-DD'.
        TABLE_UPDATE_INTERVAL: Intervalo em meses para atualização.
    Retorno:
        pd.DataFrame: DataFrame contendo os dados diários de pedidos pagos.
    """

    db_lake_comercial = mysql_lake_comercial.DatabaseConnector('analytics_performance')

    query = f"""
        SELECT
            DATE(created_at_order) AS created_order_date,
            p.nome_equipe_chave_pedido AS team,
            p.email_vendedor AS vendedor,
            COUNT(DISTINCT cg.GrupoID_final) AS quantity_clients_paid,
            ROUND(SUM(p.total_price_order),2) AS revenue_paid,
            COUNT(DISTINCT p.id_order) AS quantity_orders_paid
        FROM 
            analytics_performance.gold_pedidos_comercial_dashboard p
            INNER JOIN analytics_performance.silver_customer_groupID cg ON p.customer_id = cg.customer_id
        WHERE
            DATE(p.created_at_order) >= DATE_FORMAT(DATE_SUB('{YESTERDAY}', INTERVAL {TABLE_UPDATE_INTERVAL} MONTH), '%%Y-%%m-01')
            AND DATE(p.created_at_order) <= '{YESTERDAY}'
            AND is_paid = 1
        GROUP BY
            1,2,3
        ORDER BY
            1,2,3;
    """

    df = pd.read_sql_query(query, db_lake_comercial.create_engine(), dtype={'created_order_date': 'datetime64[ns]'})

    return df


def obter_ticket_valor_invoice_diario_vendedor()-> pd.DataFrame:
    """
    Descrição:
        Função para obter o ticket e valor total de pedidos faturados diariamente por vendedor.
    Parametros:
        YESTERDAY (str): Data de referência no formato 'YYYY-MM-DD'.
        TABLE_UPDATE_INTERVAL: Intervalo em meses para atualização.
    Retorno:
        pd.DataFrame: DataFrame contendo os dados diários de pedidos pagos.
    """

    db_lake_comercial = mysql_lake_comercial.DatabaseConnector('analytics_performance')
        
    query = f"""
        SELECT
            DATE(order_invoice_date) AS order_invoice_date,
            p.nome_equipe_chave_invoice AS team,
            p.email_vendedor AS vendedor,
            COUNT(DISTINCT cg.GrupoID_final) AS quantity_clients_invoice,
            ROUND(SUM(p.total_price_order),2) AS revenue_invoice,
            COUNT(DISTINCT p.id_order) AS quantity_orders_invoice
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

    df = pd.read_sql_query(query, db_lake_comercial.create_engine(), dtype={'order_invoice_date': 'datetime64[ns]'})

    return df