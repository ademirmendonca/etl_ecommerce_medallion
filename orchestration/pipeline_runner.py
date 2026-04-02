from datetime import datetime

# Imports das camadas
from src.bronze.ingestion_orders import BronzeIngestion
from src.silver.silver_orders_consolidated import SilverOrdersConsolidated
from src.silver.silver_payments_summary import SilverPaymentsSummary
from src.gold.gold_product_metrics import GoldProductMetrics


# Função de log
def log(mensagem):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {mensagem}")

# Bronze
def run_bronze():
    log("Iniciando etapa BRONZE")
    try:
        bronze = BronzeIngestion()

        bronze.ingestao_bronze(
            file_path="caminho/para/arquivo.csv",  # ajustar conforme necessário
            catalogo_path="delta/bronze/orders",
            tipo_carga="full"
        )

        log("Finalizado BRONZE com sucesso")

    except Exception as e:
        log(f"Erro na etapa BRONZE: {e}")


# Silver
def run_silver():
    log("Iniciando etapa SILVER")

    # Orders Consolidated
    try:
        log("Iniciando Silver - Orders Consolidated")

        silver_orders = SilverOrdersConsolidated()
        silver_orders.process()

        log("Finalizado Silver - Orders Consolidated")

    except Exception as e:
        log(f"Erro em Silver Orders Consolidated: {e}")

    # Payments Summary
    try:
        log("Iniciando Silver - Payments Summary")

        silver_payments = SilverPaymentsSummary()
        silver_payments.process()

        log("Finalizado Silver - Payments Summary")

    except Exception as e:
        log(f"Erro em Silver Payments Summary: {e}")

    log("Finalizado etapa SILVER")

# Gold
def run_gold():
    log("Iniciando etapa GOLD")

    try:
        gold = GoldProductMetrics()
        gold.process()

        log("Finalizado GOLD com sucesso")

    except Exception as e:
        log(f"Erro na etapa GOLD: {e}")


# Pipe principal
def run_pipeline():
    log(" INÍCIO DO PIPELINE ")

    run_bronze()
    run_silver()
    run_gold()

    log(" FIM DO PIPELINE ")

# Execução
if __name__ == "__main__":
    run_pipeline()


