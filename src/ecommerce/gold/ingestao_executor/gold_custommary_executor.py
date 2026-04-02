from pyspark.sql import SparkSession
from src.ecommerce.gold.ingestao.customer_summary import ETLGold

spark = SparkSession.getActiveSession()

# Ambiente
ambiente = "prd"  # prd / dev

# Fonte
df_consolidate = spark.read.table(f"workspace.silver_{ambiente}.orders_consolidated")
df_payments = spark.read.table(f"workspace.silver_{ambiente}.payments_summary")


# Destino
workspace = "workspace"
schema = f"gold_{ambiente}"
tabela = "customer_summary"
catalogo_destino = f"{workspace}.{schema}.{tabela}"

# Tipo de carga
if spark.catalog.tableExists(catalogo_destino):
    tipo_carga = "incremental"
else:
    print(f"Tabela não existe, criando.. {catalogo_destino}")
    tipo_carga = "full"


def main():
    pipe = ETLGold()
    pipe.ingestao_gold(
        df_consolidate=df_consolidate,
        df_payments=df_payments,
        catalogo_destino=catalogo_destino,
        tipo_carga=tipo_carga
    )

if __name__ == "__main__":
    main()

