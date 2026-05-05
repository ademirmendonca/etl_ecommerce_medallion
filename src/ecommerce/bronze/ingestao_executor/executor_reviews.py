from pyspark.sql import SparkSession
from src.ecommerce.bronze.ingestao.ingestao_reviews import BronzeIngestion

spark = SparkSession.getActiveSession()

# Ambiente
ambiente = "prd"  # prd / dev

# Fonte
file_path = "/Volumes/workspace/dados/raw/olist_order_reviews_dataset.csv"

# Destino
workspace = "workspace"
schema = f"bronze_{ambiente}"
tabela = "reviews"
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
    
