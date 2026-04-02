# Detalhamento -> gold
###Tabela: customer_summary
- Devido a inconsistência dos dados, já destacada na camada silver da fonte **"reviews"**, neste primeiro momento não foi considerado a avaliação do cliente junto a tabela **"customer_summary"** porque poderá comprometer consideravelmente a tomada de decisão, mas pode ser agregado posteriormente considerando a chegada de uma nova base de dados consistente.
Obs: Devido a alta volumetria de inconsistência, não é viável a manutenção da base de dados manualmente. Para mais informações, vide readme.md da camada silver.


### Tabela: product_summary

Agregado todos os valores de enriquecimento

A tabela **product_summary** foi gerada a partir da agregação dos dados de vendas, consolidando informações por categoria de produto. Foram calculados os totais de unidades vendidas, receita, número de pedidos e valor médio do frete, utilizando dados limpos e enriquecidos da camada silver.


### Tabela: seller_summary

A tabela **seller_summary** foi criada a partir da consolidação dos dados de vendas, agrupando informações por vendedor. Foram calculados indicadores como total de vendas, receita, número de pedidos e valor médio do frete, utilizando apenas dados limpos e enriquecidos da camada silver. Não foi realizado o join com a tabela **"reviews"** devido à inconsistência dos dados, evitando assim possíveis distorções nos resultados e garantindo maior confiabilidade nas análises.

