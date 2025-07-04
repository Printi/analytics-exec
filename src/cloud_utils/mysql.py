from src.db_connectors import mysql_lake_comercial
from sqlalchemy import text
import pandas as pd
from src.tabelas_indicadores import dic_dtype
import mysql.connector
from time import sleep

def atualizar_tabela_mysql(df, tabela, conexao,):
    """
    Atualiza uma tabela no MySQL com dados do DataFrame.

    Parâmetros:
        df (pd.DataFrame): DataFrame com os dados a serem inseridos.
        tabela (str): Nome da tabela no MySQL.
        conexao: Objeto/instância conexão.
    Retorno:
        None
    """

    try:
        # Conectar ao MySQL
        conn = conexao
        cursor = conn.cursor()

        cursor.execute("USE analytics_exec;")  # Garante que estamos no banco certo

        # Apagar todos os registros e inserir tudo novamente
        cursor.execute(f"DELETE FROM {tabela}")

        # Construção dinâmica da query
        colunas = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))

        query = f"""
        INSERT INTO {tabela} ({colunas})
        VALUES ({placeholders});
        """

        def split_batches(lista, tamanho_lote):
            for i in range(0, len(lista), tamanho_lote):
                yield lista[i:i + tamanho_lote]

        # Define tamanho do lote
        batch_size = 1000

        dados = df.values.tolist()

        # Divide em batches
        batches = list(split_batches(dados, batch_size))

        for batch in batches:
            cursor.executemany(query, batch)

        print(f"Tabela {tabela} substituída com {len(df)} registros.")

        conn.commit()

    except mysql.connector.Error as err:
        print(f"Erro MySQL: {err}")
        conn.rollback()
        print(f"Rollback Realizado! {err}")
    finally:
        cursor.close()

def mysql_query_rel(conexao,query: str):
    ATTEMPT_WAIT=120
    ATTEMPT=5
    cont=0
    while True:
        try:
            with conexao.cursor() as cursor:
                cursor.execute(query)
                df = pd.DataFrame(cursor.fetchall())  # `DictCursor` já retorna dicionário
                if df.empty:
                    raise Exception("DF Vazio") 
            return df
        except (mysql.connector.Error,Exception) as e:
            print(f'Erro ao executar a consulta: {e}, \n Tentativa {cont+1} de {ATTEMPT}...')
            print()
            cont+=1
            if cont==ATTEMPT:
            #    mensagem=enviar_msg_slack_canal(f"<ETL> 5 TENTATIVAS: FALHA NA CONSULTA AO BD RELACIONAMENTO :emergency: CANCELADO ")
                quit()
            else:
                print("Aguardando 120 segundos...")
                sleep(ATTEMPT_WAIT)
