import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT))

spark_session = MagicMock()
funcoes = ModuleType("pyspark.sql.functions")
setattr(funcoes, "current_timestamp", MagicMock())
types_mod = ModuleType("pyspark.sql.types")
for name in ["StructType", "StructField", "StringType", "TimestampType", "IntegerType", "FloatType"]:
    setattr(types_mod, name, MagicMock())

pyspark = ModuleType("pyspark")
pyspark_sql = ModuleType("pyspark.sql")
pyspark_sql.SparkSession = spark_session
pyspark_sql.functions = funcoes
sys.modules["pyspark"] = pyspark
sys.modules["pyspark.sql"] = pyspark_sql
sys.modules["pyspark.sql.functions"] = funcoes
sys.modules["pyspark.sql.types"] = types_mod

from src.ecommerce.bronze.ingestao.ingestao_products import BronzeIngestion


def test_ingestao_products_inicializa_spark_e_salva():
    spark_session.reset_mock()
    session = MagicMock()
    spark_session.builder.appName.return_value.getOrCreate.return_value = session

    ingestao = BronzeIngestion(app_name="teste-products")

    arquivo = "products.csv"
    tabela = "workspace.bronze_products"
    df = MagicMock()
    df.withColumn.return_value = df
    df.write.format.return_value.mode.return_value.saveAsTable.return_value = None
    session.read.csv.return_value = df

    ingestao.ingestao_bronze(arquivo, tabela, "full")

    assert session.read.csv.call_args[0][0] == arquivo
    assert session.read.csv.call_args[1]["header"] is True
    assert session.read.csv.call_args[1]["sep"] == ","
    df.withColumn.assert_called_once()
    df.write.format.assert_called_once_with("delta")
    df.write.format.return_value.mode.assert_called_once_with("append")
    df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(tabela)
