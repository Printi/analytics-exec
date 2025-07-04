from src.db_connectors import mysql_lake_comercial
import pandas as pd
from src.config import TABLE_UPDATE_INTERVAL,YESTERDAY

def obter_metas_diarias_vendedor()-> pd.DataFrame:
    """
    Descrição:
        Função para obter as metas diárias  por vendedor.
    Parametros:
        YESTERDAY (str): Data de referência no formato 'YYYY-MM-DD'.
        TABLE_UPDATE_INTERVAL: Intervalo em meses para atualização.
    Retorno:
        pd.DataFrame: DataFrame contendo as metas diárias.
    """

    db_lake_comercial = mysql_lake_comercial.DatabaseConnector('analytics_performance')

    
    #META S&OP
    # query_vendedor = f"""
    #     SELECT
    #     DATE(sop.`date`) AS 'meta_date',
    #     UPPER(sop.nome_area) AS 'channel',
    #     UPPER(sop.nome_equipe) AS 'team',
    #     sop.email_agente AS 'vendedor',
    #     ROUND(SUM(sop.receita_acordada_dia_util), 2) AS 'meta_diaria',
    #     mm.meta_mensal AS 'meta_mensal'
    # FROM
    #     analytics_performance.tb_quadro_colaboradores_meta_diaria_com_receita_acordada AS sop
    # LEFT JOIN (
    #     SELECT
    #         sop_mes.competencia,
    #         sop_mes.email_agente,
    #         ROUND(MAX(sop_mes.meta_individual_mensal), 2) AS 'meta_mensal'
    #     FROM
    #         analytics_performance.tb_quadro_colaboradores_meta_diaria_com_receita_acordada AS sop_mes
    #     WHERE
    #         DATE(sop_mes.`date`) >= DATE_FORMAT(DATE_SUB('{YESTERDAY}', INTERVAL {TABLE_UPDATE_INTERVAL} MONTH), '%%Y-%%m-01')
    #         AND DATE(sop_mes.`date`) <= '{YESTERDAY}'
    #     GROUP BY
    #         1, 2
    # ) AS mm ON
    #     mm.competencia = sop.competencia AND
    #     mm.email_agente = sop.email_agente
    # WHERE
    #     DATE(sop.`date`) >= DATE_FORMAT(DATE_SUB('{YESTERDAY}', INTERVAL {TABLE_UPDATE_INTERVAL} MONTH), '%%Y-%%m-01')
    #     AND DATE(sop.`date`) <= '{YESTERDAY}'
    # GROUP BY
    #     1, 2, 3, 4
    # ORDER BY
    #     1, 2, 3, 4;
    # """

    # local_df_metas_diarias = pd.read_sql_query(query_vendedor, db_lake_comercial.create_engine())
    
    # return local_df_metas_diarias


    query_vendedor = f"""
        SELECT
        DATE(meta.`date`) AS 'meta_date',
        UPPER(meta.nome_area) AS 'channel',
        UPPER(meta.nome_equipe) AS 'team',
        meta.email_agente AS 'vendedor',
        ROUND(SUM(meta.meta_dia_util), 2) AS 'meta_diaria',
        meta.meta_individual_mensal AS 'meta_mensal'
    FROM
        analytics_performance.tb_quadro_colaboradores_meta_diaria AS meta
    WHERE
        DATE(meta.`date`) >= DATE_FORMAT(DATE_SUB('{YESTERDAY}', INTERVAL {TABLE_UPDATE_INTERVAL} MONTH), '%%Y-%%m-01')
        AND DATE(meta.`date`) <= '{YESTERDAY}'
    GROUP BY
        1, 2, 3, 4
    ORDER BY
        1, 2, 3, 4;
    """

    local_df_metas_diarias = pd.read_sql_query(query_vendedor, db_lake_comercial.create_engine())
    
    return local_df_metas_diarias