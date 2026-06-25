# 🚀 Pipeline ETL para Análise de E-commerce com Olist Dataset

![Python](https://img.shields.io/badge/Python-3.14+-blue?style=for-the-badge\&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue?style=for-the-badge\&logo=postgresql)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-150458?style=for-the-badge\&logo=pandas)
![uv](https://img.shields.io/badge/uv-Package%20Manager-orange?style=for-the-badge)

![GitHub Repo stars](https://img.shields.io/github/stars/wedleycso/analise_ecommerce?style=for-the-badge)
![GitHub last commit](https://img.shields.io/github/last-commit/wedleycso/analise_ecommerce?style=for-the-badge)

---

## 📖 Sobre o Projeto

Este projeto implementa um pipeline modular de **ETL (Extract, Transform, Load)** utilizando o conjunto de dados de e-commerce da Olist.

O objetivo é realizar a extração de dados provenientes de arquivos CSV, aplicar transformações para limpeza e padronização das informações e, posteriormente, armazenar os dados em um banco PostgreSQL estruturado para análises futuras.

O pipeline foi desenvolvido seguindo boas práticas de Engenharia de Dados, incluindo:

* ✅ **Modularização:** Código dividido em pacotes de configuração, banco e pipeline (`src/`)
* ✅ **Orquestração Única:** Ponto de entrada centralizado para execução do fluxo
* ✅ **Segurança:** Usuário dedicado, evitando exposição do superusuário do sistema
* ✅ **Performance:** Inserções em lote (*Bulk Insert*) utilizando `executemany()`
* ✅ **Idempotência:** Tratamento de duplicidades com cláusulas estruturadas
* ✅ **Integridade Referencial:** Relacionamentos protegidos por chaves estrangeiras
* ✅ **Containerização:** Ambiente de banco de dados isolado via Docker Compose

---

# 🛠️ Stack Tecnológica

| Tecnologia     | Finalidade                                |
| -------------- | ----------------------------------------- |
| Python 3.14+   | Desenvolvimento do pipeline               |
| Pandas         | Extração e transformação dos dados        |
| Psycopg 3      | Comunicação com PostgreSQL                |
| PostgreSQL 18  | Armazenamento dos dados                   |
| Docker Compose | Infraestrutura do banco de dados          |
| uv             | Gerenciamento de dependências e ambientes |

---

# 📂 Estrutura do Projeto

```text
analise_ecommerce/
│
├── base_de_dados/              # Arquivos CSV brutos (Ignorados pelo Git)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   └── ...
│
├── src/
│   ├── __init__.py
│   ├── config.py               # Configurações e caminhos do projeto
│   ├── database.py             # Criação e gerenciamento das tabelas
│   └── pipeline.py             # Funções de extração, transformação e carga
│
├── docker-compose.yml          # Infraestrutura PostgreSQL
├── .gitignore
├── .python-version
├── pyproject.toml
├── uv.lock
├── run_pipeline.py             # Orquestrador principal
└── README.md
```

---

# 🏗️ Arquitetura da Solução

```text
 [ CSV Files ] (base_de_dados/)
        │
        ▼
 [ Extração ] (src/pipeline.py → Pandas)
        │
        ▼
 [ Transformação ] (Data Cleaning & Timestamps)
        │
        ▼
 [ Carga em Lote ] (Psycopg 3 → Bulk Insert)
        │
        ▼
 [ PostgreSQL Container ] (Docker Compose)
```

A arquitetura foi projetada para utilizar um usuário dedicado ao projeto e um ambiente isolado via Docker, garantindo consistência entre desenvolvimento, testes e futuras implantações.

---

# ⚙️ Pré-requisitos

Antes de iniciar, certifique-se de possuir:

* Docker e Docker Compose instalados
* Python 3.14+
* uv instalado

## Instalação do uv

### Linux / macOS

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Windows

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

---

# 🗄️ Configuração do Banco de Dados (Docker)

O projeto utiliza o **Docker Compose** para isolar o ambiente de dados, garantindo que o PostgreSQL 18 execute de forma idêntica em qualquer máquina.

## 1. Subir a Infraestrutura

Certifique-se de que o Docker está instalado e ativo na sua máquina.

Na raiz do projeto, execute:

```bash
docker compose up -d
```

O Docker criará automaticamente:

* Banco de dados `ecommerce`
* Usuário `olist_user`
* Senha padrão `admin`
* Porta `5432` exposta localmente

---

## 2. Verificar o Status do Container

Para garantir que o banco de dados foi inicializado corretamente:

```bash
docker ps
```

Você deverá visualizar um container semelhante a:

```text
olist_postgres_container
```

com status **Up**.

---

# 📥 Clonando o Projeto

```bash
git clone https://github.com/wedleycso/analise_ecommerce.git

cd analise_ecommerce
```

---

# 📦 Instalação das Dependências

O projeto utiliza o **uv** para gerenciamento de dependências e ambientes virtuais.

Sincronize o ambiente:

```bash
uv sync
```

O `uv` utilizará automaticamente os arquivos `.python-version` e `pyproject.toml` para configurar o ambiente de execução.

---

# 🚀 Executando o Projeto

Com o container PostgreSQL ativo, execute o pipeline:

```bash
uv run run_pipeline.py
```

O orquestrador central será responsável por:

* Criar ou validar a estrutura do banco de dados
* Executar a carga da tabela `olist_orders`
* Executar a carga da tabela `olist_order_items`
* Garantir a sequência correta das operações
* Inserir os dados diretamente no PostgreSQL executando dentro do container Docker

---

# 🗃️ Entidades Ingeridas

## olist_orders

Dados principais do fluxo de pedidos:

* ID do pedido (`order_id`)
* ID do cliente (`customer_id`)
* Status do pedido (`order_status`)
* Data da compra (`order_purchase_timestamp`)

---

## olist_order_items

Itens associados a cada pedido:

* ID do pedido
* ID do produto
* ID do vendedor
* Preço do produto
* Valor do frete
* Número sequencial do item

Características da modelagem:

* Chaves compostas
* Chaves estrangeiras
* Integridade referencial
* `ON DELETE CASCADE`

---

# 📈 Possíveis Evoluções

* [ ] Apache Airflow para orquestração
* [ ] Data Warehouse dimensional
* [ ] Dashboards com Power BI
* [ ] Dashboards com Streamlit
* [ ] Testes automatizados
* [ ] Integração com AWS S3
* [ ] Pipeline CI/CD
* [ ] Camada de validação de qualidade dos dados (Data Quality)
* [ ] Monitoramento e observabilidade do pipeline
* [ ] Data Lake para armazenamento bruto dos dados

---

# 🎯 Objetivo Educacional

Este projeto foi desenvolvido com foco no aprendizado de conceitos fundamentais de:

* Engenharia de Dados
* ETL
* Banco de Dados Relacional
* PostgreSQL
* Docker
* Python para Dados
* Modelagem de Dados
* Integração entre aplicações e banco de dados
* Arquitetura de pipelines modulares

---

# 📌 Versionamento

O projeto segue os princípios de **Versionamento Semântico (SemVer)**:

```text
MAJOR.MINOR.PATCH
```

Versão atual:

```text
v0.3.1
```

---

# 👨‍💻 Autor

**Wédley C. Oliveira**

🎓 Analista e Desenvolvedor de Sistemas

📊 Focado em Engenharia de Dados, Análise de Dados e Desenvolvimento Backend.

### Contato

* GitHub: https://github.com/wedleycso

---

⭐ Se este projeto foi útil para você, considere deixar uma estrela no repositório.
