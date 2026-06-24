import pandas as pd
import psycopg

#Aqui ocorrerá a extração dos dados CSV
df_pedidos = pd.read_csv('base_de_dados/olist_orders_dataset.csv')

#================================================

#Aqui será realizada a transformação dos dados

#Convertendo a string de data para o formato TIMESTAMP do PostgreSQL
df_pedidos['order_purchase_timestamp'] = pd.to_datetime(df_pedidos['order_purchase_timestamp'])

#Selecionado as colunas que serão inseridas 
df_selecionado = df_pedidos[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp']]

#Removendo linhas com valores nulos
df_limpo = df_selecionado.dropna()

#conversão do DF para uma lista de tuplas (padrão do psycopg)
registros_para_inserir = [tuple(x) for x in df_limpo.to_numpy()] 

#================================================

#Aqui ocorrerá a carga dos dados no banco de dados

DB_CONFIG = 'host=127.0.0.1 dbname=ecommerce user=olist_user password=admin'

sql_insert = '''
    INSERT INTO olist_orders (order_id, customer_id, order_status, order_purchase_timestamp)
    VALUES (%s, %s, %s, %s) -- Garante que os dados sejam inseridos corretamente
    ON CONFLICT (order_id) DO NOTHING; -- Evita duplicidade de registros
'''

print('Inserindo registros no banco de dados...')
with psycopg.connect(DB_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.executemany(sql_insert, registros_para_inserir)
        print('Dados inseridos com sucesso!!!')