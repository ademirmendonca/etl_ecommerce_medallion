from pyspark.sql import SparkSession
from src.ecommerce.silver.ingestao.silver_orders_consolidate import ETLSilver

spark = SparkSession.getActiveSession()

# Ambiente
ambiente = "prd"  # prd / dev

# Fonte
df_orders = spark.read.table(f"workspace.bronze_{ambiente}.orders")
df_orders_items = spark.read.table(f"workspace.bronze_{ambiente}.orders_items")
df_customers = spark.read.table(f"workspace.bronze_{ambiente}.customers")
df_products = spark.read.table(f"workspace.bronze_{ambiente}.products")
df_selleas = spark.read.table(f"workspace.bronze_{ambiente}.sellers")
df_reviews = spark.read.table(f"workspace.bronze_{ambiente}.reviews")

# Destino
workspace = "workspace"
schema = f"silver_{ambiente}"
tabela = "orders_consolidated"
catalogo_destino = f"{workspace}.{schema}.{tabela}"

# Tipo de carga
if spark.catalog.tableExists(catalogo_destino):
    tipo_carga = "incremental"
else:
    print(f"Tabela não existe, criando.. {catalogo_destino}")
    tipo_carga = "full"


def main():
    pipe = ETLSilver()
    pipe.ingestao_silver(
        df_orders=df_orders,
        df_orders_items=df_orders_items,
        df_customers=df_customers,
        df_products=df_products,
        df_selleas=df_selleas,
        df_reviews=df_reviews,
        catalogo_destino=catalogo_destino,
        tipo_carga=tipo_carga
    )

if __name__ == "__main__":
    main()

