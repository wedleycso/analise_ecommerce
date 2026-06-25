import os

#Passando a configuração do banco para esse arquivo
DB_CONFIG = 'host=127.0.0.1 port=5432 dbname=ecommerce user=olist_user password=admin'

#Caminhos para os arquivos CSV
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'base_de_dados')

PATH_ORDERS = os.path.join(DATA_DIR, 'olist_orders_dataset.csv')
PATH_ITEMS = os.path.join(DATA_DIR, 'olist_order_items_dataset.csv')