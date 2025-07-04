from joblib import Parallel, delayed
from src.db_queries import obter_quadro_executivos,obter_calendario,obter_cotacoes,obter_metas,obter_pedidos,obter_receita,obter_ticket_clientes
from src.utils.func import print_log,formatar_df
import pandas as pd
from sqlalchemy.types import DATE, VARCHAR, DECIMAL, INT

print_log('Gerando os dataframes com processamento paralelo...', level='terminal_process')

# Lista de funções com delayed (não executa ainda)
funcoes_coletoras_dataframes = [
    delayed(obter_quadro_executivos.obter_quadro_diario_colaboradores)(),
    delayed(obter_metas.obter_metas_diarias_vendedor)(),
    delayed(obter_receita.obter_receita_diaria_ajustada_vendedor)(),
    delayed(obter_cotacoes.obter_cotacoes_diarias_vendedor)(),
    delayed(obter_pedidos.obter_pedidos_diario_vendedor)(),
    delayed(obter_ticket_clientes.obter_ticket_valor_pago_diario_vendedor)(),
    delayed(obter_ticket_clientes.obter_ticket_valor_invoice_diario_vendedor)()
    ]

# Execução em paralelo (n_jobs=-1 usa todos os núcleos disponíveis)
lista_dataframes_coletados = Parallel(n_jobs=-1)(funcoes_coletoras_dataframes)

print_log('Estruturando os dataframes...', level='terminal_process')

lista_dataframes_coletados=formatar_df(lista_dataframes_coletados)

### TABELAS DIMENSAO

## TABELA RECEITA FATURADA E META - INVOICE
def tabela_receita_invoice():
    """
    Realiza o merge de dados de metas diárias/mensais com indicadores de receita faturada (invoice).

    Esta função combina informações de:
    - Metas de venda (diárias e mensais) por data, vendedor, canal e equipe.
    - Indicadores de receita faturada (quantidade de pedidos, clientes e valor da receita)
      com base na data e no vendedor.

    As colunas são renomeadas para padronização e o DataFrame final é reordenado.

    Parâmetros:
    lista_dataframes_coletados (list[pd.DataFrame]): Uma lista contendo os DataFrames coletados.
                                                   Esperado que os índices 1 e 2 contenham,
                                                   respectivamente, os DataFrames de metas e receita faturada.

    Retorna:
    pd.DataFrame: Um DataFrame consolidado com metas e receita faturada por dia, vendedor, canal e equipe.
    """

    df_receita_fat_meta=pd.merge(lista_dataframes_coletados[1][['meta_date','vendedor','channel',
                                                                'team','meta_diaria','meta_mensal']],
                                lista_dataframes_coletados[2][['invoice_date','vendedor',
                                                            'quantity_order','quantity_clients',
                                                            'receita']],
                                left_on=['meta_date','vendedor'],
                                right_on=['invoice_date','vendedor'],
                                how='left')

    df_receita_fat_meta.drop(columns=['invoice_date'],inplace=True)

    df_receita_fat_meta.rename(columns={
                                'meta_date':'data',
                                'quantity_order':'quantidade_pedidos_invoice',
                                'quantity_clients':'quantidade_clientes_pedidos_invoice',
                                'receita':'receita_invoice'},inplace=True)

    cols_order=['data','channel','team','vendedor', 
                'quantidade_pedidos_invoice', 'quantidade_clientes_pedidos_invoice',
                'receita_invoice', 'meta_diaria','meta_mensal']

    df_receita_fat_meta=df_receita_fat_meta[cols_order]

    return df_receita_fat_meta


## TABELA DE COTACOES E PEDIDOS - CRIADOS 
def tabela_cotacoes_pedidos():
    """
    Consolida dados de cotações criadas e pedidos criados com informações do quadro de colaboradores.

    A função realiza dois merges sequenciais:
    1. Mescla o DataFrame base (quadro de colaboradores) com dados de cotações, usando
       a data e o vendedor como chaves de junção.
    2. Mescla o resultado anterior com dados de pedidos criados, também usando data e vendedor.

    As colunas são renomeadas para padronização e colunas redundantes são removidas.

    Parâmetros:
    lista_dataframes_coletados (list[pd.DataFrame]): Uma lista contendo os DataFrames coletados.
                                                   Esperado que os índices 0, 3 e 4 contenham,
                                                   respectivamente, os DataFrames de quadro de colaboradores,
                                                   cotações e pedidos criados.

    Retorna:
    pd.DataFrame: Um DataFrame consolidado com informações de cotações e pedidos criados
                  por data e vendedor.
    """

    df_cot=lista_dataframes_coletados[0].merge(lista_dataframes_coletados[3],
                                        left_on=['data','vendedor'],
                                        right_on=['quotation_created_date',
                                                'email_responsavel_cotacao'],how='left')
    df_cot['team'] = df_cot.groupby('vendedor')['team'].ffill()
    df_cot.rename(columns={'quantity_quotation': 'quantidade_cotacoes',
                        'quantity_clients':'quantidade_cotacoes_clientes_unicos'},inplace=True)

    df_cot_ped=df_cot.merge(lista_dataframes_coletados[4],
                            left_on=['data','vendedor','team'],
                            right_on=['created_order_date','vendedor','team'],
                            how='left')

    df_cot_ped.rename(columns={'quantity_order':'quantidade_pedidos_criados',
                            'quantity_order_with_quotation':'quantidade_pedidos_criados_com_cotacao',
                            'quantity_clients':'quantidade_clientes_pedidos_criados',
                            'receita':'receita_criada',
                            'team_x':'team'},inplace=True)
                    
    df_cot_ped.drop(columns=['quotation_created_date','email_responsavel_cotacao',
                            'created_order_date'],inplace=True)

    return df_cot_ped


## TABELA DE PEDIDOS - PAGOS
def tabela_pedidos_pagos():
    """
    Consolida dados de pedidos pagos com informações do quadro de colaboradores.

    A função realiza um merge do DataFrame base (quadro de colaboradores) com
    dados de pedidos pagos, utilizando a data de criação do pedido e o vendedor
    como chaves de junção. As colunas são renomeadas e reordenadas para
    apresentação final.

    Parâmetros:
    lista_dataframes_coletados (list[pd.DataFrame]): Uma lista contendo os DataFrames coletados.
                                                   Esperado que os índices 0 e 5 contenham,
                                                   respectivamente, os DataFrames de quadro de colaboradores
                                                   e de ticket/valor pago.

    Retorna:
    pd.DataFrame: Um DataFrame consolidado com informações de pedidos pagos
                  por data, vendedor e equipe.
    """
    df_pedidos_pagos=lista_dataframes_coletados[0].merge(lista_dataframes_coletados[5],
                                            left_on=['data','vendedor'],
                                            right_on=['created_order_date',
                                                    'vendedor'],how='left')

    df_pedidos_pagos.rename(columns={'quantity_clients_paid':'quantidade_clientes_pedidos_pagos',
                            'quantity_orders_paid':'quantidade_pedidos_pagos',
                            'quantity_clients':'quantidade_clientes_pedidos_criados',
                            'revenue_paid':'receita_paga',
                            'team_x':'team'},inplace=True)

    df_pedidos_pagos.drop(columns=['created_order_date'],inplace=True)
    cols_order=['data', 'vendedor', 'team',
                'quantidade_pedidos_pagos' ,'quantidade_clientes_pedidos_pagos',
                'receita_paga']
    
    df_pedidos_pagos=df_pedidos_pagos[cols_order]
    return df_pedidos_pagos


### TABELA FATO
def consolidar_criar_fato(df_criados:pd.DataFrame,df_pagos:pd.DataFrame,df_invoice:pd.DataFrame)->pd.DataFrame:
    df=df_criados.merge(df_pagos,on=['data','vendedor','team'],how='left') 
    df=df.merge(df_invoice,on=['data','vendedor','team'],how='left')
    #df.drop(columns=['team_x','team_y'],inplace=True)
    df['usuario']=None
    cols_order=['data','usuario','channel','team', 'vendedor', 'quantidade_cotacoes',
       'quantidade_cotacoes_clientes_unicos', 'valor_cotacoes',
       'quantidade_pedidos_criados', 'quantidade_pedidos_criados_com_cotacao',
       'quantidade_clientes_pedidos_criados', 'receita_criada',
       'quantidade_pedidos_pagos', 'quantidade_clientes_pedidos_pagos',
       'receita_paga', 'quantidade_pedidos_invoice',
       'quantidade_clientes_pedidos_invoice', 'receita_invoice', 'meta_diaria',
       'meta_mensal']
    df=df[cols_order]
    df_formatado=formatar_df([df])
    return df_formatado


def criar_tabelas_indicadores():
    df_criado=formatar_df([tabela_cotacoes_pedidos()])
    df_pago=formatar_df([tabela_pedidos_pagos()])
    df_invoice=formatar_df([tabela_receita_invoice()])
    df_fato=consolidar_criar_fato(df_criado,df_pago,df_invoice)

    return [df_criado,df_pago,df_invoice,df_fato]


dtype_mapping_tb_dimensao_criados = {
    'data': DATE,
    'vendedor': VARCHAR(255),
    'team': VARCHAR(100),
    'quantidade_cotacoes': INT,
    'quantidade_cotacoes_clientes_unicos': INT,
    'valor_cotacoes': DECIMAL(precision=12, scale=2),
    'quantidade_pedidos_criados': INT,
    'quantidade_pedidos_criados_com_cotacao': INT,
    'quantidade_clientes_pedidos_criados': INT,
    'receita_criada': DECIMAL(precision=12, scale=2)
}

dtype_mapping_tb_dimensao_pagos = {
    'data': DATE,
    'vendedor': VARCHAR(255),
    'team': VARCHAR(100),
    'quantidade_pedidos_pagos': INT,
    'quantidade_clientes_pedidos_pagos': INT,
    'receita_paga': DECIMAL(precision=12, scale=2)
}

dtype_mapping_tb_dimensao_invoice = {
    'data': DATE,
    'channel': VARCHAR(100),
    'team': VARCHAR(100),
    'vendedor': VARCHAR(255),
    'quantidade_pedidos_invoice': INT,
    'quantidade_clientes_pedidos_invoice': INT,
    'receita_invoice': DECIMAL(precision=12, scale=2),
    'meta_diaria': DECIMAL(precision=12, scale=2),
    'meta_mensal': DECIMAL(precision=12, scale=2)
}

dtype_mapping_fato_indicadores_diarios = {
    'data': DATE,
    'usuario': VARCHAR(100),  # Nova coluna, assumindo VARCHAR(100)
    'vendedor': VARCHAR(255), # Ajustado para 255 para e-mail
    'channel': VARCHAR(100),
    'team': VARCHAR(100),
    'quantidade_cotacoes': INT,
    'quantidade_cotacoes_clientes_unicos': INT,
    'valor_cotacoes': DECIMAL(precision=12, scale=2),
    'quantidade_pedidos_criados': INT,
    'quantidade_pedidos_criados_com_cotacao': INT,
    'quantidade_clientes_pedidos_criados': INT,
    'receita_criada': DECIMAL(precision=12, scale=2),
    'quantidade_pedidos_pagos': INT,
    'quantidade_clientes_pedidos_pagos': INT,
    'receita_paga': DECIMAL(precision=12, scale=2),
    'quantidade_pedidos_invoice': INT,
    'quantidade_clientes_pedidos_invoice': INT,
    'receita_invoice': DECIMAL(precision=12, scale=2),
    'meta_diaria': DECIMAL(precision=12, scale=2),
    'meta_mensal': DECIMAL(precision=12, scale=2)

}

dic_dtype={'tb_dimensao_criados':dtype_mapping_tb_dimensao_criados,
    'tb_dimensao_pagos':dtype_mapping_tb_dimensao_pagos,
    'tb_dimensao_invoice':dtype_mapping_tb_dimensao_invoice,
    'fato_indicadores_diarios':dtype_mapping_fato_indicadores_diarios}