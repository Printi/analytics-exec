from datetime import datetime
import pytz
import pandas as pd
import numpy as np

def print_log(message, level="info"):
    """
    Print a formatted log message with a specified severity level.

    Args:
        message (str): The message to be logged.
        level (str): The severity level of the message. 
        Options are "success", "warning", "error", "terminal_process", or "info" (default).
    """
    # Define log levels and corresponding colors
    levels = {
        "success": ("[SUCESSO]", "\033[92m"),  # Green
        "warning": ("[ATENÇÃO]", "\033[93m"),  # Yellow
        "error": ("[ERRO]", "\033[91m"),       # Red
        "info": ("[INFO]", "\033[94m"),        # Blue
        "terminal_process": ("[EXEC]", "\037") # Gray
    }

    # Get the prefix and color for the given level, defaulting to "info" if level is invalid
    prefix, color = levels.get(level.lower(), levels["info"])

    # Reset color
    reset_color = "\033[0m"

    # Get current date and time with timezone
    tz = pytz.timezone('America/Sao_Paulo')  # You can change this to your preferred timezone
    current_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    # Print the formatted message
    print(f"{color}{current_time} {prefix} {message}{reset_color}")

    import pandas as pd

def formatar_df(df_list:list[pd.DataFrame])-> list[pd.DataFrame]:
    
    new_df_list=[]
    for df in df_list:
        df_temp=df.copy()

        #TRATAMENTO DE DATA
        for col in df.columns.tolist():
            if ('data' in col.lower()) or ('date' in col.lower()):

                df_temp[col]= pd.to_datetime(df_temp[col],format='%Y-%m-%d',errors='coerce').dt.strftime('%Y-%m-%d')
        
        #TRATAMENTO DE NAN
        df_temp.replace(np.nan,None,inplace=True)        
        
        new_df_list.append(df_temp)
    if len(new_df_list)==1:
        return new_df_list[0]
    else:
        return new_df_list

def merge_and_filter_workdays(dataframes: dict[str, pd.DataFrame],calendar_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Realiza o merge de múltiplos DataFrames com um DataFrame de calendário
    para adicionar a coluna 'work_day' e, em seguida, filtra cada DataFrame
    para incluir apenas os dias úteis.

    Parâmetros:
    dataframes (dict[str, pd.DataFrame]): Um dicionário onde as chaves são nomes
                                          (para referência) e os valores são os DataFrames a serem processados.
    calendar_df (pd.DataFrame): O DataFrame que contém a coluna 'date' (data do calendário)
                                e 'work_day' (booleano indicando dia útil).
    Retorna:
    dict[str, pd.DataFrame]: Um dicionário com os DataFrames processados e filtrados por dia útil.
    """

    processed_dataframes = {}

    for name, df in dataframes.items():
        # 1. Obter o nome da coluna de data para o DataFrame atual
        df_date_col = list(df.columns)[0]
        if not df_date_col:
            print(f"Aviso: Coluna de data para '{name}' não encontrada no 'date_column_map'. Pulando este DataFrame.")
            processed_dataframes[name] = df # Retorna o DF original sem modificação
            continue

        # Garantir que as colunas de data estejam no formato datetime para o merge
        df[df_date_col] = pd.to_datetime(df[df_date_col])
        calendar_df['date'] = pd.to_datetime(calendar_df['date'])

        # 2. Realizar o merge com o DataFrame de calendário
        # Usa um left merge para manter todas as linhas do DF original e adicionar 'work_day'
        merged_df = pd.merge(
            df,
            calendar_df[['date', 'work_day']], # Seleciona apenas 'date' e 'work_day' do calendário
            left_on=df_date_col,
            right_on='date',
            how='left'
        )

        # Remove a coluna 'date' duplicada que vem do calendar_df se ela tiver o mesmo nome
        # Isso é importante se a coluna de merge 'df_date_col' já for chamada 'date'
        if df_date_col != 'date' and 'date' in merged_df.columns:
             merged_df = merged_df.drop(columns=['date'])

        # Verificar se a coluna 'work_day' foi adicionada corretamente
        if 'work_day' not in merged_df.columns:
            print(f"Erro: 'work_day' não foi adicionada a '{name}' após o merge. Verifique os nomes das colunas de data.")
            processed_dataframes[name] = df # Retorna o DF original sem modificação
            continue
        
        # Lidar com possíveis NaNs na coluna work_day após o merge (ocorre se não houver match)
        # Por simplicidade, vamos assumir que dias sem match não são úteis.
        merged_df['work_day'] = merged_df['work_day'].fillna(0)


        # 3. Filtrar o DataFrame para manter apenas os dias úteis
        filtered_df = merged_df[merged_df['work_day'] == 1].copy()
        
        # Opcional: Remover a coluna 'work_day' se ela não for mais necessária após o filtro
        # filtered_df = filtered_df.drop(columns=['work_day'])

        processed_dataframes[name] = filtered_df
        
    return processed_dataframes