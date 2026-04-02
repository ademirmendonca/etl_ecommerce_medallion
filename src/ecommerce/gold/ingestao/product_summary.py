from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

class ETLGold:
    def __init__(self, app_name="etl-gold-product-sumary"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Iniciando processamento, product-sumary..")

    def ingestao_gold(self, df_consolidate, df_payments, catalogo_destino, tipo_carga):
        print(f"Processando novos dados, carga: {tipo_carga}")

        try:

            df_final = (
                df_consolidate.join(df_payments, df_consolidate.order_id == df_payments.order_id)
                .groupBy(df_consolidate.product_category_name)
                .agg(
                    F.count(df_consolidate.order_item_id).alias("total_units_sold"),
                    F.sum(F.col("price") + F.col("freight_value")).alias("total_revenue"),
                    F.countDistinct(df_consolidate.order_id).alias("total_orders"),
                    F.round(F.avg(F.col("freight_value")), 2).alias("avg_freight_value")
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


