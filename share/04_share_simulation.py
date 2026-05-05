from pyspark.sql import functions as F


# Leitura das tabelas Gold
df_customer = spark.table("workspace.gold_prd.customer_summary")
df_product  = spark.table("workspace.gold_prd.product_summary")
df_seller   = spark.table("workspace.gold_prd.seller_summary")


#Exporta para CSV
output_path = "/Volumes/workspace/gold_prd/output/"

(df_customer
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .csv(f"{output_path}gold_customer_summary_export.csv"))

(df_product
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .csv(f"{output_path}gold_product_summary_export.csv"))

# Preparado para us
(df_seller
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .csv(f"{output_path}gold_seller_summary_export.csv"))


#Resumo executivo
# Total de clientes únicos atendidos
total_customers = df_customer.select("customer_id").distinct().count()

# Receita total consolidada (usando customer como fonte principal)
total_revenue = df_customer.agg(
    F.sum("total_revenue").alias("total_revenue")
).collect()[0]["total_revenue"]

# Top 3 categorias por receita
top3_categories = (df_product
    .groupBy("product_category_name")
    .agg(F.sum("total_revenue").alias("revenue"))
    .orderBy(F.desc("revenue"))
    .limit(3)
    .collect()
)

# Estado com maior volume de pedidos
top_state = (df_customer
    .groupBy("customer_state")
    .agg(F.sum("total_orders").alias("orders"))
    .orderBy(F.desc("orders"))
    .limit(1)
    .collect()[0]
)

# Vendedor com melhor "avaliação média"
# OBS: como não existe coluna de avaliação no seller_summary,
# usamos proxy: receita média por pedido (total_revenue / total_orders)
top_seller = (df_seller
    .filter(F.col("total_orders") >= 10)
    .withColumn("media_pedido", F.col("total_revenue") / F.col("total_orders"))
    .select("seller_id", "total_revenue", "total_orders", "media_pedido", "avg_review_score")
    .orderBy(F.desc("media_pedido"))
    .limit(1)
    .collect()[0]
)

# Resumo executivo
print("Resumo executivo!")
print(f"Total de clientes únicos atendidos: {total_customers}")
print(f"Receita total consolidada R$: {total_revenue}")

print("\nTop 3 categorias por receita:")
for row in top3_categories:
    print(f"- {row['product_category_name']} R$: {row['revenue']}")

print("\nEstado com maior volume de pedidos:")
print(f"- {top_state['customer_state']}: {top_state['orders']} pedidos")

print("\nVendedor com melhor performance, 10 ou mais pedidos:")
print(f"- Vendedor ID: {top_seller['seller_id']}")
print(f"  Receita total R$: {top_seller['total_revenue']}")
print(f"  Pedidos: {top_seller['total_orders']}")
print(f"  Receita média por pedido R$: {round(top_seller['media_pedido'], 2)}")
print(f"  Avaliação média: {round(top_seller['avg_review_score'], 2)}")


