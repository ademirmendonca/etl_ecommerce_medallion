# Descritivo do projeto

## Diagrama da arquitetura (texto resumido e ASCII)

Arquitetura de Dados — Fluxo Completo (Diagrama da arquitetura)

A solução foi construída seguindo o padrão Medallion Architecture (Bronze → Silver → Gold), com uma camada adicional de orquestração e uma simulação de compartilhamento de dados (Data Sharing).

 - Orquestração (Pipeline)

      O processo é controlado por um script de orquestração (pipeline_runner.py), que simula o comportamento do Azure Data Factory.

      Esse script é responsável por:

      Executar as etapas na ordem correta: Bronze → Silver → Gold
      Registrar logs de início e fim de cada etapa com timestamp
      Obs.: **Conforme orientação no enunciado**, caso aconteça alguma falha, entã a falha será registrada, porém não irá impedir o processamentos das demais etapas!

###- Camada Bronze - Ingestão

A camada Bronze é responsável pela ingestão dos dados brutos a partir de arquivos CSV.

Entrada:
Arquivos de origem (raw), como: customers, orders, order_items, payments, products, sellers
Processamento: Aplicação de schema explícito
Inclusão da coluna ingestion_timestamp para rastreabilidade e identificar data/hora em que houve o processamento
Tabelas Delta armazenadas no catálogo, tais como:
Costumers: "workspace.bronze_prd.customers"
Orders: "workspace.bronze_prd.orders"
Orders Itens: "workspace.bronze_prd.orders_items"
Payments: "workspace.bronze_prd.payments"
Products: "workspace.bronze_prd.products"
Reviews: "workspace.bronze_prd.reviews"
Sellers: "workspace.bronze_prd.sellers"

Obs.: Visando garantir a performance e otimização de custos, foi definido a tipagem dos dados diretamente da camada bronze (aplicado somente a tipagem dos dados, evitando transformações na camada bronze.)


###- Camada Silver - Transformações e consolidação

A camada Silver realiza o tratamento e a organização dos dados.

Entrada:
Tabelas da camada Bronze (mencionadas acima)

Processos principais:
1. Orders Consolidated
Join entre:
orders
order_items
customers
products
sellers
Aplicação de filtro:
Apenas pedidos com status delivered ou shipped
Remoção de colunas duplicadas
Padronização dos dados

2. Payments Summary
Agregação dos dados de pagamentos por order_id
Cálculo de métricas como:
Valor total pago
Quantidade de parcelas
Tipos de pagamento utilizados

3. Identificado inconsistência nos dados da fonte "olist_order_reviews_dataset.csv", para mais detalhes, consulte o README.md na camada Silver (**considerando que a liderança e equipe de negócios foram avisados**)

Catálogo:
Dados consolidados: "workspace.silver_prd.orders_consolidated"
Payments summary: "workspace.silver_prd.payments_summary"


###- Camada Gold — Agregações de Negócio

A camada Gold é voltada para consumo analítico e tomada de decisão.
Entrada:
Tabelas da camada Silver (mencionadas acima)

Processamentos:
1. Product Summary
Total de unidades vendidas
Receita total
Quantidade de pedidos
Frete médio
2. Customer Summary
Total de pedidos por cliente
Distribuição por estado
3. Seller Summary
Total de pedidos por vendedor
Receita total por vendedor

Catálogo:
Customer Summary: "workspace.gold_prd.customer_summary"
Product Summary: "workspace.gold_prd.product_summary"
Saller Summary: "workspace.gold_prd.seller_summary"
Essas tabelas são otimizadas para consumo por ferramentas de BI.


###- Simulação de Data Sharing

Foi implementado o script share/04_share_simulation.py para simular o consumo externo dos dados, como ocorreria com Delta Sharing.

Funcionalidades:
Leitura das tabelas Gold:
customer_summary
product_summary
seller_summary

Posteriormente imprime os resultados no promsalva os arquivos em formato ".csv" na pasta share/output/ sendo eles:
gold_seller_summary_export.csv
gold_product_summary_export.csv
gold_customer_summary_export.csv


        +---------------------+
        |  Orquestração       |
        |  pipeline_runner.py |
        +----------+----------+
                   |
                 Bronze
          +----------------+
          | Ingestão CSV   |
          | raw → Delta    |
          | customers,     |
          | orders, etc.   |
          +----------------+
                   |
                 Silver
          +----------------+
          | Transformação & |
          | Consolidação    |
          | orders,         |
          | payments,       |
          | reviews         |
          +----------------+
                   |
                 Gold
          +----------------+
          | Agregações      |
          | Customer,       |
          | Product, Seller |
          +----------------+
                   |
            Data Sharing
       +---------------------+
       | Export CSV / BI     |
       +---------------------+



### Decisões de design
1- Atendendo a necessidade negócio, o script foi desenvolvido visando remover os dados duplicados da tabela de pedidos utilizando o método `.dropDuplicates(["order_id"])`, garantindo que cada pedido seja processado apenas uma vez.

2- Foram descartados pedidos com status diferente de "delivered" ou "shipped" através do filtro `.filter((F.col("order_status") == "delivered") | (F.col("order_status") == "shipped"))`, pois apenas pedidos entregues ou enviados são relevantes para análises de vendas e performance.

3- Para cálculo de receita total, foi considerado o valor do produto somado ao valor do frete, utilizando as colunas `price` e `freight_value` da tabela de itens do pedido, garantindo que o custo total refletisse o valor efetivamente pago pelo cliente.


### Como rodar o projeto

1. Instale as dependências
Recomenda-se usar um ambiente virtual (ex: `venv` ou `conda`).

bash
pip install -r pyproject.toml/requirements.txt

2. Prepare os arquivos de dados
Coloque os arquivos CSV originais do Olist na pasta:
data/raw/
Exemplo de arquivos esperados:
- olist_customers_dataset.csv
- olist_orders_dataset.csv
- olist_order_items_dataset.csv
- olist_order_payments_dataset.csv
- olist_products_dataset.csv
- olist_order_reviews_dataset.csv
- olist_sellers_dataset.csv

3. Execute o pipeline de orquestração
O script principal é o `pipeline_runner.py`. Ele executa todas as etapas (Bronze → Silver → Gold), embora seja possível executar o processo manualmente via arquivos que terminal com o nome "executer" nas respectivas camadas.

bash
python pipeline_runner.py

4. (Opcional) Simule o compartilhamento de dados
Para exportar as tabelas Gold em CSV, execute:

bash
python share/04_share_simulation.py
Os arquivos exportados serão salvos em:
share/output/

Obs.: Caso tenha dificuldades, pode-se salvar os arquivos em um volume, como alternativa.


5. Consulte os resultados
- As tabelas Delta são salvas no catálogo conforme descrito na arquitetura.
- Os arquivos CSV exportados podem ser usados em ferramentas de BI.


### O que mudaria em produção

Em um ambiente real de produção com **Databricks + Azure Data Factory + Delta Sharing**, existem diferenças significativas em relação à implementação local:

**Implementação Local:**
- Sem métricas de performance detalhadas
- Versionamento manual
- Execução em máquina local com recursos limitados (CPU, memória)
- Metadados básicos
- Execução manual via linha de comando

**Ambiente Real (Azure Data Factory):**
- Triggers automáticos: agendamento (cron)
- Alertas automáticos via email etc..
- Versionamento automático: time travel e auditoria completa de acessos
- Linhagem de dados: visualização gráfica de dependências upstream/downstream
- Catálogo centralizado

### Limitações
1- Utilização do autoloader para processamento automático na chegada de novos arquivos
2- Exploração mais profunda dos dados para sugestão de melhorias para a área de negócio
3 - Particionamento e otimização de tabelas Delta.


Dúvidas? Verifique os demais README.md nas camadas silver e gold.

Email: ademir_mendonca@hotmail.com


Até breve.
Ademir Mendonça!
