from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType, FloatType)
from pyspark.sql import functions as F


class BronzeIngestion:
    def __init__(self, app_name="list_products"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Iniciando processamento..")

    def ingestao_bronze(self, file_path, catalogo_path, tipo_carga):
        print(f"Processando arquivo: {file_path}, carga: {tipo_carga}")

        try:
            schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("product_category_name", StringType(), True),
                StructField("product_name_lenght", IntegerType(), True),
                StructField("product_description_lenght", IntegerType(), True),
                StructField("product_photos_qty", IntegerType(), True),
                StructField("product_weight_g", FloatType(), True),
                StructField("product_length_cm", FloatType(), True),
                StructField("product_height_cm", FloatType(), True),
                StructField("product_width_cm", FloatType(), True),
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


