from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class ETLGold:
    def __init__(self, app_name="etl-gold-customer-summary"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Iniciando processamento, customer-summary..")

    def ingestao_gold(self, df_consolidate, df_payments, catalogo_destino, tipo_carga):
        print(f"Processando novos dados, carga: {tipo_carga}")

        try:

            df_final = (
                df_consolidate.join(df_payments, df_consolidate.order_id == df_payments.order_id)
                .groupBy(
                    df_consolidate.customer_id.alias("customer_id"),
                    df_consolidate.customer_city.alias("customer_city"),
                    df_consolidate.customer_state.alias("customer_state")
                )
                .agg(
                    F.count(df_consolidate.order_id).alias("total_orders"),
                    F.sum(df_consolidate.price + df_consolidate.freight_value).alias("total_revenue"),
                    F.round(F.avg(df_consolidate.price + df_consolidate.freight_value), 2).alias("avg_order_value"),
                    F.round(F.avg(df_consolidate.review_score.cast("double")), 2).cast("decimal(3,2)").alias("avg_review_score")
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


