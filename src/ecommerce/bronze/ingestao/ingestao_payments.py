from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DecimalType, TimestampType)
from pyspark.sql import functions as F


class BronzeIngestion:
    def __init__(self, app_name="list_payments"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Iniciando processamento..")

    def ingestao_bronze(self, file_path, catalogo_path, tipo_carga):
        print(f"Processando arquivo: {file_path}, carga: {tipo_carga}")

        try:
            schema = StructType([
                StructField("order_id", StringType(), True),
                StructField("payment_sequential", IntegerType(), True),
                StructField("payment_type", StringType(), True),
                StructField("payment_installments", IntegerType(), True),
                StructField("payment_value", DecimalType(10, 2), True),
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


