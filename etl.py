import pandas as pd
import psycopg

# as três etapas do ETL
#1. Extrair os dados do arquivo CSV
df_pedidos = pd.read_csv('olist_orders_dataset.csv')

#2. Transformar os dados (aqui eu ajusto o formato da data para o formato correto do PostgreSQL)
df_pedidos['data_compra '] = pd.to_datetime(df_pedidos['data_compra'])

#3. Agora é feita a carga de dados para o banco de dados PostgreSQL
DB_CONFIG = "dbname=ecommerce user=postgres password=admin"

#essa instrução sql serve para inserir os dados na tebela com espaços reservados (%s) contra injeção de SQL
sql_insert = """
    INSERT INTO pedidos (id_pedido, id_cliente, data_compra)
    VALUES (%s, %s, %s)
"""

#aqui converto as colunas são convertidas para uma lista
registros_para_inserir = df_pedidos[['id_pedido', 'id_cliente', 'data_compra']].values.tolist()

#aqui abre a  ligação com db e executa 
with psycopg.connect(DB_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.executemany(sql_insert, registros_para_inserir)
        print('Dados inseridos com sucesso!')