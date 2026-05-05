# Detalhamento -> gold

### Tabela: product_summary

Agregado todos os valores de enriquecimento

A tabela **product_summary** foi gerada a partir da agregação dos dados de vendas, consolidando informações por categoria de produto. Foram calculados os totais de unidades vendidas, receita, número de pedidos e valor médio do frete, utilizando dados limpos e enriquecidos da camada silver.


### Tabela: seller_summary

A tabela **seller_summary** foi criada a partir da consolidação dos dados de vendas, agrupando informações por vendedor. Foram calculados indicadores como total de vendas, receita, número de pedidos e valor médio do frete, utilizando apenas dados limpos e enriquecidos da camada silver. Não foi realizado o join com a tabela **"reviews"** devido à inconsistência dos dados, evitando assim possíveis distorções nos resultados e garantindo maior confiabilidade nas análises.

