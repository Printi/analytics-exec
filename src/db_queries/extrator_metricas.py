from src.db_connectors import mysql_lake_comercial
import pandas as pd


def extrair_metricas_por_equipes(df: pd.DataFrame, col_metricas: list, equipes: list) -> dict:
    """
    Extrai as métricas especificadas para cada equipe do DataFrame.
    Retorna um dicionário com as equipes como chaves e métricas como valores (dict).
    """
    resultado = {}
    for equipe in equipes:
        dados = df[df['team'] == equipe.upper()]
        if not dados.empty:
            metricas = {col: float(dados[col].sum()) for col in col_metricas if col in dados.columns}
            resultado[equipe.upper()] = metricas
        else:
            resultado[equipe.upper()] = {col: 0.0 for col in col_metricas}
    return resultado

