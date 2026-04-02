from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class ETLGold:
    def __init__(self, app_name="etl-gold-seller-sumary"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Iniciando processamento, seller-sumary..")

    def ingestao_gold(self, df_consolidate, df_payments, catalogo_destino, tipo_carga):
        print(f"Processando novos dados, carga: {tipo_carga}")

        try:

            df_final = (
                df_consolidate.join(df_payments, df_consolidate.order_id == df_payments.order_id)
                .groupBy(
                    df_consolidate.seller_id,
                    df_consolidate.seller_city,
                    df_consolidate.seller_state
                )
                .agg(
                    F.countDistinct(df_consolidate.order_id).alias("total_orders"),
                    F.sum(df_payments.total_pago).alias("total_revenue"),
                )
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


