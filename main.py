from src.tabelas_indicadores import criar_tabelas_indicadores
from pandas import concat
from src.cloud_utils.mysql import atualizar_tabela_mysql
from src.db_connectors.mysql_simple_connector import simple_connection
from src.google_drive_utils import connect_list_csv_and_download
from src.tabelas_hubspot import process_data_hub

#==============================ANALYTICS-PERFORMANCE==============================

#CRIAR TABELAS DE INDICADORES
df_criado,df_pago,df_invoice,df_fato=criar_tabelas_indicadores()

#SET USUARIOS COMUNS (EXECUTIVOS)
df_fato['usuario']=df_fato['vendedor']

#SET USUARIOS ADMIN
USERS=['joaof.santos@printi.com.br','regiane.silva@printi.com.br',
       'eduardo.feliciano@printi.com.br','erick.lima@printi.com.br',
       'guilherme.mattos@printi.com.br','layanie.almeida@printi.com.br']

df_temp_users=[df_fato]
for admin_user in USERS:
    df_temp_admin=df_fato.copy()
    df_temp_admin['usuario']=admin_user
    df_temp_users.append(df_temp_admin)

#GERAR FATO
df_fato=concat(df_temp_users)


#SETAR USUÁRIOS SUPERVISÃO (COMBINAÇÕES DE SUPERVISOR E TIME)
ADMS_INBOUND=['priscila.viacelli@printi.com.br','juliana.silva@printi.com.br','tais.souza@printi.com.br']
ADMS_FARMERS=['kate.ramos@printi.com.br','ruan.santo@printi.com.br','lenize.caetano@printi.com.br']

dict_area={0:'INBOUND',1:'FARMERS'}

dados_admin_area=[df_fato]
for index,AREA in enumerate([ADMS_INBOUND,ADMS_FARMERS]):
    for admin_user in AREA:
        df_temp_admin=df_fato[~df_fato['usuario'].isin([USERS])].copy() #SEM USERS ADMIN GERAIS
        df_temp_admin=df_temp_admin[df_temp_admin['team']==dict_area[index]]
        df_temp_admin['usuario']=admin_user
        dados_admin_area.append(df_temp_admin)

df_fato=concat(dados_admin_area)

#ATUALIZAR TABELAS NO MYSQL
dic_tables={
    'tb_dimensao_criados':df_criado,
    'tb_dimensao_pagos':df_pago,
    'tb_dimensao_invoice':df_invoice,
    'fato_indicadores_diarios':df_fato
    }

conn=simple_connection()
for table,df in dic_tables.items():
    atualizar_tabela_mysql(df,table,conn)

#=================================HUBSPOT======================================
#OBTER DADOS DO HUBSPOT
connect_list_csv_and_download()
list_data_hubspot=process_data_hub(conn)

# SETAR CONFIGURAÇÃO DE USERS E ADMIN
for index_1,data in enumerate(list_data_hubspot):
    #SET USUARIOS COMUNS (EXECUTIVOS)
    data['USERS']=data['OWNER_EMAIL']

    #SET USUARIOS ADMIN
    df_temp_users=[data]
    for admin_user in USERS:
        df_temp_admin=data.copy()
        df_temp_admin['USERS']=admin_user
        df_temp_users.append(df_temp_admin)

    df_fato=concat(df_temp_users)
    dados_admin_area=[df_fato]
    for index_2,AREA in enumerate([ADMS_INBOUND,ADMS_FARMERS]):
        for admin_user in AREA:
            df_temp_admin=df_fato[~df_fato['USERS'].isin([USERS])].copy() #SEM USERS ADMIN GERAIS
            df_temp_admin=df_temp_admin[df_temp_admin['TEAM']==dict_area[index_2]]
            df_temp_admin['USERS']=admin_user
            dados_admin_area.append(df_temp_admin)

    list_data_hubspot[index_1]=concat(dados_admin_area)


#ATUALIZAR TABELAS NO MYSQL
dic_tables_hub={
    'tb_dimensao_hubspot_whats_email':list_data_hubspot[0],
    'tb_dimensao_hubspot_lp':list_data_hubspot[1]}

for table,df in dic_tables_hub.items():
    atualizar_tabela_mysql(df,table,conn)