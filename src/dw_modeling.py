import logging
from src.config import get_db_connection

#Configurando o Logging para acompanhar a execução
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_gold_schema(cursor) -> None:
    '''Cria o schema Gold isolado se ele não existir'''
    logger.info('Criando o schema "Gold" se não existir..')
    cursor.execute('CREATE SCHEMA IF NOT EXISTS gold;')

def build_dim_tempo(cursor) -> None:
    '''Cria a tabela dim_tempo de forma automatizada no schema Gold'''
    logger.info('Construindo a tabela "gold.dim_tempo"...')

    cursor.execute('DROP TABLE IF EXISTS gold.dim_tempo;')

    create_table_query = '''
    CREATE TABLE gold.dim_tempo AS
    SELECT
        TO_CHAR(data_serie, 'YYYYMMDDD'):: INT AS tempo_sk, -- Surrogate Key (Chave Inteira)
        data_serie::DATE AS data_completa,
        EXTRACT(YEAR FROM data_serie)::INT AS ano,
        EXTRACT(MONTH FROM data_serie)::INT AS mes,
        TO_CHAR(data_serie, 'TMMonth') AS nome_mes, -- Nome do mês em português
        EXTRACT(DAY FROM data_serie)::INT AS dia,
        EXTRACT(QUARTER FROM data_serie)::INT AS trimestre,
        EXTRACT(ISODOW FROM data_serie)::INT AS dia_semana, --1 (Segunda) a 7 (Domingo)
        CASE
            WHEN EXTRACT (ISODOW FROM data_serie) IN (6,7) THEN TRUE
            ELSE FALSE
        END AS eh_final_semana
    FROM GENERATE_SERIES(
        '2016-01-01'::TIMESTAMP,
        '2020-12-31'::TIMESTAMP,
        '1 day'::INTERVAL
    ) AS data_serie;

    -- Criação de índice para garantir performance nos JOINS ALTER TABLE gold.dim_tempo ADD PRIMARY KEY (tempo_sk);
    '''

    cursor.execute(create_table_query)
    logger.info('Tabela "gold.dim_tempo" criada e populada com sucesso!')

def run_dw_modeling():
    '''
    Orquestrador principal da transformação de dados.
    '''
    logger.info('Iniciando o processo de Modelagem Dimensional (Gold)...')

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                #Cria o ambiente isolado
                create_gold_schema(cursor)

                #Constrói as dimensões
                build_dim_tempo(cursor)

                #Garantindo que as alterações sejam salvas no banco
                conn.commit()

            logger.info('Modelagem Dimensional concluída com sucesso!')
        
    except Exception as e:
        logger.error(f'Erro crítico durante a modelagem : {e}')
        raise e
    
# ... (mantenha as funções anteriores: create_gold_schema e build_dim_tempo)

def build_fato_vendas(cursor) -> None:
    '''Gera a tabela fato_vendas cruzando as tabelas operacionais e vinculando à dim_tempo.'''
    logger.info("Construindo a tabela 'gold.fato_vendas'...")
    
    # Garantindo a idempotência
    cursor.execute("DROP TABLE IF EXISTS gold.fato_vendas CASCADE;")
    
    # Query que consolida as métricas e chaves analíticas
    create_table_query = '''
    CREATE TABLE gold.fato_vendas AS
    SELECT 
        oi.order_id,
        oi.product_id,
        -- Vinculando a data do pedido à Surrogate Key da dim_tempo
        TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INT AS data_pedido_sk,
        
        -- Métricas Numéricas
        oi.price AS preco,
        oi.freight_value AS frete_valor,
        (oi.price + oi.freight_value) AS faturamento_total_item,
        
        -- Contexto operacional útil para auditoria
        o.order_status
    FROM public.olist_order_items oi
    INNER JOIN public.olist_orders o ON oi.order_id = o.order_id
    -- Filtro de governança: levamos para o DW apenas pedidos válidos
    WHERE o.order_status NOT IN ('canceled', 'unavailable');
    
    -- Índices para performance máxima em relatórios futuros
    CREATE INDEX idx_fato_vendas_produto ON gold.fato_vendas(product_id);
    CREATE INDEX idx_fato_vendas_tempo ON gold.fato_vendas(data_pedido_sk);'''
    cursor.execute(create_table_query)
    logger.info("Tabela 'gold.fato_vendas' criada e populada com sucesso!")

def run_dw_modeling():
    '''Orquestrador principal da transformação de dados.'''
    logger.info("Iniciando o processo de Modelagem Dimensional (Gold)...")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                #Cria o ambiente isolado
                create_gold_schema(cursor)
                
                #Constrói as Dimensões
                build_dim_tempo(cursor)
                
                #Constrói a Fato (NOVA ETAPA)
                build_fato_vendas(cursor)
                
                #Garante que tudo seja gravado
                conn.commit()
                
        logger.info("Modelagem Dimensional concluída com sucesso total! 🚀")
        
    except Exception as e:
        logger.error(f"Erro crítico durante a modelagem: {e}")
        raise e

if __name__ == "__main__":
    run_dw_modeling()
