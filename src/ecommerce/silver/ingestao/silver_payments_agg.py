from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class ETLSilver:
    def __init__(self, app_name="etl-silver-payments-agg"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Iniciando processamento etl-silver-payments-agg..")

    def ingestao_silver(self, df_payments, catalogo_destino, tipo_carga):
        print(f"Processando novos dados, carga: {tipo_carga}")

        try:
            df_final = (
                df_payments.groupBy("order_id")
                .agg(
                    F.sum("payment_value").alias("total_pago"),
                    F.sum("payment_installments").alias("quantidade_parcelas"),
                    F.collect_set("payment_type").alias("formas_pagamento_utilizadas")
                )
                .withColumn("ingestion_timestamp", F.current_timestamp())
            )            

            (
                df_final
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(catalogo_destino)
            )
            print(f"Processamento finalizado com sucesso em: {catalogo_destino}")
        except Exception as e:
            print(f"Erro ao processar novos dados: {e}")


