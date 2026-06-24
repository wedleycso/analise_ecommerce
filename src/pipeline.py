import pandas as pd
import psycopg
from src.config import DB_CONFIG, PATH_ORDERS, PATH_ITEMS

def executar_etl_pedidos():
    #Aqui ocorrerá a extração dos dados CSV
    print('ETL PEDIDOS: Iniciando a extração...')
    df = pd.read_csv(PATH_ORDERS)

    print('ETL PEDIDOS: Transformando os dados...')
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df_limpo = df[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp']].dropna()
    registros = [tuple(x) for x in df_limpo.to_numpy()]

    sql = '''
        INSERT INTO olist_orders (order_id, customer_id, order_status, order_purchase_timestamp)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING;
    '''

    print('ETL PEDIDOS: Carregando no banco...')
    with psycopg.connect(DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, registros)
    print('ETL PEDIDOS: Carga concluída com sucesso!')

def executar_etl_itens():
    # extração dos dados CSV
    print('ETL ITENS: Iniciando a extração...')
    df = pd.read_csv(PATH_ITEMS)

    print('ETL ITENS: Transformando os dados...')
    df_limpo = df[['order_id', 'product_id', 'order_item_id', 'price', 'freight_value']].dropna()
    registros = [tuple(x) for x in df_limpo.to_numpy()]

    sql = '''
        INSERT INTO olist_order_items (order_id, product_id, order_item_id, price, freight_value)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (order_id, product_id, order_item_id) DO NOTHING;
    '''

    print('ETL ITENS: Carregando no banco...')
    with psycopg.connect(DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, registros)
    print('ETL ITENS: Carga concluída com sucesso!')