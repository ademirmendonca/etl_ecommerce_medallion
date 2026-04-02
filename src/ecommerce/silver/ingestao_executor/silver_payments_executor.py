from pyspark.sql import SparkSession
from src.ecommerce.silver.ingestao.silver_payments_agg import ETLSilver

spark = SparkSession.getActiveSession()

# Ambiente
ambiente = "prd"  # prd / dev

# Fonte
df_payments = spark.read.table(f"workspace.bronze_{ambiente}.payments")

# Destino
workspace = "workspace"
schema = f"silver_{ambiente}"
tabela = "payments_summary"
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
        df_payments=df_payments,
        catalogo_destino=catalogo_destino,
        tipo_carga=tipo_carga
    )

if __name__ == "__main__":
    main()
