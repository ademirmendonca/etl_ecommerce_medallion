from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType)
from pyspark.sql import functions as F


class BronzeIngestion:
    def __init__(self, app_name="list_customers"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Iniciando processamento..")

    def ingestao_bronze(self, file_path, catalogo_path, tipo_carga):
        print(f"Processando arquivo: {file_path}, carga: {tipo_carga}")

        try:
            schema = StructType([
                StructField("customer_id", StringType(), True),
                StructField("customer_unique_id", StringType(), True),
                StructField("customer_zip_code_prefix", StringType(), True), # CEP pode conter zero à esquerda
                StructField("customer_city", StringType(), True),
                StructField("customer_state", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), True)
            ])

            df = self.spark.read.csv(file_path, schema=schema, header=True, sep=",")

            df = (
                df.withColumn("ingestion_timestamp", F.current_timestamp())
            )

            (
                df
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(catalogo_path)
            )
            print(f"Processamento finalizado com sucesso em: {catalogo_path}")
        except Exception as e:
            print(f"Erro ao processar novos dados: {e}")


