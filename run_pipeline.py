from src.database import criar_tabelas
from src.pipeline import executar_etl_pedidos, executar_etl_itens

def main():
    print('=============================================')
    print('INICIANDO O PROCESSO DE ETL DO E-COMMERCE')
    print('=============================================')

    #Garantindo a infraestrutura do banco de dados
    criar_tabelas()
    print('\n---------------------------------------------\n')

    #Executando a ingestão dos pedidos
    executar_etl_pedidos()
    print('\n---------------------------------------------\n')

    #Executando a ingestão dos itens
    executar_etl_itens()

    print('=============================================')
    print('FINALIZANDO O PROCESSO DE ETL DO E-COMMERCE')
    print('=============================================')
    
if __name__ == '__main__':
    main()