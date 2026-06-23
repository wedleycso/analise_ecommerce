import psycopg2

#config do banco de dados
DB_CONFIG = 'dbname=ecommerce user=postgres password=rootadmin'

def criar_tabelas():
    # Aqui o 'with' é usado para garantir que a conexão encerre corretamente.
    with psycopg2.connect(DB_CONFIG) as conn:
        with conn.cursor() as cur:

            #o cur.execute() é usado para executar os comandos SQL
            cur.execute('''
                CREATE TEBLE IF NOT EXISTS olist_orders (
                    order_id VARCHAR(50) PRIMARU KEY,
                    customer_id VARCHAR(50),
                    order_status VARCHAR(50),
                    order_purchase_timestamp TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS olist_order_itmes (
                    order_id VARCHAR(50),
                    product_id VARCHAR(50),
                    order_item_id INT, -- Aqui vai pegar o id do item no mesmo pedido
                    price DECIMAL(10,2),
                    freight_value DECIMAL(10,2),
                    PRIMARY KEY (order_id, product_id, order_item_id) -- chave composta para garantir que não haja duplicidade de itens
                    FOREIGN KEY (order_id) REFRENCES olist_orders(order_id) ON DELETE CASCADE
                );                
            ''')
            print('Tabelas criadas com sucesso!')

if __name__ == '__main__':
    criar_tabelas()