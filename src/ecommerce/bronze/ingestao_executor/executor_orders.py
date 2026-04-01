# import sys
# sys.path.append("/Workspace/Users/ademir.mendonca.teste@gmail.com/pipiline_teste_tecnico")

from pyspark.sql import SparkSession
from src.ecommerce.bronze.ingestao.ingestao_orders import BronzeIngestion

spark = SparkSession.getActiveSession()

# Ambiente
ambiente = "prd"  # prd / dev

# Fonte
file_path = "/Workspace/Users/ademir.mendonca.teste@gmail.com/pipiline_teste_tecnico/data/raw/olist_orders_dataset.csv"

# Destino
workspace = "workspace"  # ajuste conforme seu ambiente
schema = f"bronze_{ambiente}"
tabela = "orders"
catalogo_path = f"{workspace}.{schema}.{tabela}"

# Tipo de carga
if spark.catalog.tableExists(catalogo_path):
    tipo_carga = "incremental"
else:
    print(f"Tabela não existe, criando.. {catalogo_path}")
    tipo_carga = "full"


def main():
    pipe = BronzeIngestion()

    pipe.ingestao_bronze(
        file_path=file_path,
        catalogo_path=catalogo_path,
        tipo_carga=tipo_carga
    )

if __name__ == "__main__":
    main()
