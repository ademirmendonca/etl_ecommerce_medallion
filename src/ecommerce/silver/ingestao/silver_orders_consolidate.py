from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

class ETLSilver:
    def __init__(self, app_name="etl-silver"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("Iniciando processamento etl-silver..")

    def ingestao_silver(self, df_orders, df_orders_items, df_customers, df_products, df_selleas, df_reviews, catalogo_destino, tipo_carga):
        print(f"Processando novos dados, carga: {tipo_carga}")

        try:
            # Dropa os dados com valores nulos e duplicados
            df_orders = (
                df_orders
                .na.drop(subset=["order_id", "customer_id"])
                .dropDuplicates(["order_id"])
                .filter((F.col("order_status") == "delivered") | (F.col("order_status") == "shipped"))
            )

            df_join = (
                df_orders.alias("o")
                .join(df_orders_items.alias("oi"), "order_id", "inner")
                .join(df_customers.alias("c"), "customer_id", "inner")
                .join(df_products.alias("p"), "product_id", "inner")
                .join(df_selleas.alias("s"), "seller_id", "inner")
                .join(df_reviews.alias("r"), "order_id", "inner" )
            )

            df_final = df_join.select(
                # orders
                "o.order_id",
                "o.customer_id",
                "o.order_status",
                "o.order_purchase_timestamp",
                "o.order_approved_at",
                "o.order_delivered_carrier_date",
                "o.order_delivered_customer_date",
                "o.order_estimated_delivery_date",
                
                # order_items
                "oi.order_item_id",
                "oi.product_id",
                "oi.seller_id",
                "oi.shipping_limit_date",
                "oi.price",
                "oi.freight_value",

                # customers
                "c.customer_unique_id",
                "c.customer_zip_code_prefix",
                "c.customer_city",
                "c.customer_state",

                # products
                "p.product_category_name",
                "p.product_name_lenght",
                "p.product_description_lenght",
                "p.product_photos_qty",
                "p.product_weight_g",
                "p.product_length_cm",
                "p.product_height_cm",
                "p.product_width_cm",

                # sellers
                "s.seller_zip_code_prefix",
                "s.seller_city",
                "s.seller_state",

                # orders reviews
                "r.review_score",
                "r.review_comment_title",
                "r.review_comment_message",
                "r.review_creation_date",
                "r.review_answer_timestamp",

                # controle (uma única)
                F.current_timestamp().alias("ingestion_timestamp")
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


