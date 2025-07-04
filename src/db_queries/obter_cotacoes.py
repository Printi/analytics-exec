from src.db_connectors import mysql_lake_comercial
import pandas as pd
from src.config import TABLE_UPDATE_INTERVAL,YESTERDAY



def obter_cotacoes_diarias_vendedor()-> pd.DataFrame:

    """
    Descrição:
        Função para obter as cotações diárias por vendedor.
    Parametros:
        YESTERDAY (str): Data de referência no formato 'YYYY-MM-DD'.
        TABLE_UPDATE_INTERVAL: Intervalo em meses para atualização.
    Retorno:
        pd.DataFrame: DataFrame contendo as metas diárias.
    """

    db_lake_comercial = mysql_lake_comercial.DatabaseConnector('analytics_performance')

    query = f"""
        WITH cte_cotacoes AS (
            SELECT
                DATE(cot.created_at) AS 'quotation_created_date'
                ,CASE
                        WHEN UPPER(q.nome_equipe) IN ('FARMERS & KEY ACCOUNTS', 'FARMERS', 'CS - ONGOING') THEN 'FARMERS' 
                        WHEN UPPER(q.nome_equipe) IN ('PROJETOS', 'GRANDES CONTAS') THEN 'GRANDES CONTAS'
                        WHEN UPPER(q.nome_equipe) IN ('4B') THEN '4B' ELSE 'INBOUND' END AS team
                ,cot.email_responsavel_cotacao
                ,COUNT(distinct cot.id_quotation) AS 'quantity_quotation'
                ,COUNT(distinct g.GrupoID_final) AS 'quantity_clients'
                ,ROUND(SUM(cot.total_price),2) AS 'valor_cotacoes'
            FROM 
                analytics_performance.tb_alpha_cotacoes AS cot
                LEFT JOIN analytics_performance.silver_customer_groupID AS g on cot.customer_id = g.customer_id
                LEFT JOIN analytics_performance.tb_quadro_colaboradores AS q on q.chave_email_data = concat(cot.email_vendedor, '|', date(created_at))
            WHERE 
                DATE(cot.created_at) >= DATE_FORMAT(DATE_SUB('{YESTERDAY}', INTERVAL {TABLE_UPDATE_INTERVAL} MONTH), '%%Y-%%m-01')
                AND DATE(cot.created_at) <= '{YESTERDAY}'
            GROUP BY 
                1,2,3
            ORDER BY 
                1,2,3
        )
        SELECT
            quotation_created_date
            ,team
            ,email_responsavel_cotacao
            ,quantity_quotation
            ,quantity_clients
            ,valor_cotacoes
        FROM
            cte_cotacoes
    """

    df = pd.read_sql_query(query, db_lake_comercial.create_engine(), dtype=None)

    return df