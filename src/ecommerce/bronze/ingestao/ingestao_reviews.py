from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DateType, TimestampType)
from pyspark.sql import functions as F


class BronzeIngestion:
    def __init__(self, app_name="list_reviews"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Iniciando processamento..")

    def ingestao_bronze(self, file_path, catalogo_path, tipo_carga):
        print(f"Processando arquivo: {file_path}, carga: {tipo_carga}")

        try:
            schema = StructType([
                StructField("review_id", StringType(), True),
                StructField("order_id", StringType(), True),
                StructField("review_score", IntegerType(), True), 
                StructField("review_comment_title", StringType(), True),
                StructField("review_comment_message", StringType(), True),
                StructField("review_creation_date", DateType(), True), 
                StructField("review_answer_timestamp", StringType(), True),
                StructField("ingestion_timestamp", TimestampType(), True)
            ])

            df = self.spark.read.csv(file_path, schema=schema, header=True, sep=",", multiLine=True)

            df = (
                df.withColumn("ingestion_timestamp", F.current_timestamp())
            )
            df.display()
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


