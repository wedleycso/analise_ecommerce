import psycopg

# aqui é feita a configuração do banco de dados local, nesse caso sera o PostegreSQL
DB_CONFIG = "dbname=ecommerce user=postgres password=admin"

def criar_tabelas():
    # 'with' serve para abrir a conexão com a base de dados e garante que seja fechada ao final
    with psycopg.connect(DB_CONFIG) as conn:
        with conn.cursor() as cur:
            #SQL para criar as tabelas
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pedidos (
                    id_pedido VARCHAR(50) PRIMARY KEY,
                    id_cliente VARCHAR(50),
                    data_compra TIMESTAMP,
            );
                        
                CREATE TABLE IF NOT EXISTS itens_pedidos (
                        id_item SERIAL PRIMARY KEY,
                        id_pedido VARCHAR(50),
                        id_produto VARCHAR(50),
                        quantidade INT,
                        valor DECIMAL(10,2)
            );
            """)
            print('Tabelas criadas com sucesso!')

if __name__ == "__main__":
    criar_tabelas()