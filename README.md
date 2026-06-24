# 🚀 Pipeline ETL para Análise de E-commerce com Olist Dataset

![Python](https://img.shields.io/badge/Python-3.14+-blue?style=for-the-badge\&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue?style=for-the-badge\&logo=postgresql)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-150458?style=for-the-badge\&logo=pandas)
![uv](https://img.shields.io/badge/uv-Package%20Manager-orange?style=for-the-badge)

![GitHub Repo stars](https://img.shields.io/github/stars/wedleycso/analise_ecommerce?style=for-the-badge)
![GitHub last commit](https://img.shields.io/github/last-commit/wedleycso/analise_ecommerce?style=for-the-badge)

---

## 📖 Sobre o Projeto

Este projeto implementa um pipeline completo de **ETL (Extract, Transform, Load)** utilizando o conjunto de dados de e-commerce da Olist.

O objetivo é realizar a extração de dados provenientes de arquivos CSV, aplicar transformações para limpeza e padronização das informações e, posteriormente, armazenar os dados em um banco PostgreSQL estruturado para análises futuras.

O pipeline foi desenvolvido seguindo boas práticas de Engenharia de Dados, incluindo:

* ✅ Separação das etapas de ETL
* ✅ Banco de dados relacional normalizado
* ✅ Integridade referencial
* ✅ Inserções em lote (Bulk Insert)
* ✅ Tratamento de duplicidades
* ✅ Ambiente isolado para desenvolvimento

---

# 🛠️ Stack Tecnológica

| Tecnologia    | Finalidade                                |
| ------------- | ----------------------------------------- |
| Python 3.14+  | Desenvolvimento do pipeline               |
| Pandas        | Extração e transformação dos dados        |
| Psycopg 3     | Comunicação com PostgreSQL                |
| PostgreSQL 18 | Armazenamento dos dados                   |
| uv            | Gerenciamento de dependências e ambientes |

---

# 📂 Estrutura do Projeto

```text
analise_ecommerce/
│
├── base_de_dados/
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   └── ...
│
├── setup_banco.py
├── etl.py
├── pyproject.toml
├── uv.lock
└── README.md
```

---

# 🏗️ Arquitetura da Solução

```text
CSV Files
    │
    ▼
Extração (Pandas)
    │
    ▼
Transformação
(Limpeza e Padronização)
    │
    ▼
Carga (Psycopg)
    │
    ▼
PostgreSQL
```

A arquitetura foi projetada para utilizar um usuário dedicado ao projeto, evitando o uso direto do superusuário `postgres`, aumentando a segurança e facilitando a administração do ambiente.

---

# ⚙️ Pré-requisitos

Antes de iniciar, certifique-se de possuir:

* PostgreSQL 15 ou superior
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

# 🗄️ Configuração do Banco de Dados

## 1. Criar o banco

```bash
sudo -u postgres createdb ecommerce
```

## 2. Criar usuário dedicado

Durante a execução será solicitada uma senha.

```bash
sudo -u postgres createuser olist_user --pwprompt
```

Sugestão para ambiente local:

```text
admin
```

## 3. Conceder permissões

```bash
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE ecommerce TO olist_user;"
```

---

# 📥 Clonando o Projeto

```bash
git clone https://github.com/wedleycso/analise_ecommerce.git

cd analise_ecommerce
```

---

# 📦 Instalação das Dependências

O projeto utiliza o **uv** para gerenciamento de dependências.

Para sincronizar o ambiente:

```bash
uv sync
```

---

# 🚀 Executando o Projeto

## Etapa 1 — Criação das Tabelas

O script abaixo cria toda a estrutura relacional do banco de dados:

* Chaves primárias
* Chaves estrangeiras
* Chaves compostas
* Integridade referencial
* ON DELETE CASCADE

```bash
uv run setup_banco.py
```

---

## Etapa 2 — Execução do Pipeline ETL

O script realiza:

### Extração

* Leitura dos arquivos CSV
* Carregamento para DataFrames Pandas

### Transformação

* Remoção de registros inválidos
* Conversão de datas
* Padronização dos tipos de dados
* Tratamento de valores nulos

### Carga

* Inserção em massa (Bulk Insert)
* Uso de `executemany()`
* Tratamento de duplicidades com:

```sql
ON CONFLICT DO NOTHING
```

Execução:

```bash
uv run etl.py
```

---

# 🗃️ Estrutura das Tabelas

Atualmente o pipeline realiza a carga de dados para as seguintes tabelas:

## olist_orders

Informações principais dos pedidos:

* ID do pedido (`order_id`)
* ID do cliente (`customer_id`)
* Status do pedido (`order_status`)
* Data da compra (`order_purchase_timestamp`)

## olist_order_items

Informações dos itens vendidos:

* ID do pedido
* ID do produto
* ID do vendedor
* Preço do produto
* Valor do frete
* Número sequencial do item no pedido

> **Observação:** As tabelas poderão ser expandidas futuramente para incluir novos atributos disponíveis no dataset original da Olist, como datas de aprovação, entrega e estimativa de entrega.

---

# 📈 Possíveis Evoluções

* [ ] Docker Compose para PostgreSQL
* [ ] Apache Airflow para orquestração
* [ ] Data Warehouse dimensional
* [ ] Dashboards com Power BI
* [ ] Dashboards com Streamlit
* [ ] Testes automatizados
* [ ] Integração com AWS S3
* [ ] Pipeline CI/CD
* [ ] Camada de validação de qualidade dos dados (Data Quality)

---

# 🎯 Objetivo Educacional

Este projeto foi desenvolvido com foco no aprendizado de conceitos fundamentais de:

* Engenharia de Dados
* ETL
* Banco de Dados Relacional
* PostgreSQL
* Python para Dados
* Modelagem de Dados
* Integração entre aplicações e banco de dados

---

# 👨‍💻 Autor

**Wédley C. Oliveira**

🎓 Analista e Desenvolvedor de Sistemas

📊 Focado em Engenharia de Dados, Análise de Dados e Desenvolvimento Backend.


---

⭐ Se este projeto foi útil para você, considere deixar uma estrela no repositório.
