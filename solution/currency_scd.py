from pyspark.sql.functions import col,lit,current_timestamp,upper,replace
from pyspark.sql.types import IntegerType, TimestampType, DecimalType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def build_currency_scd(bronze_path: str, silver_path: str):
    f1 = bronze_path+"/currency_codes.csv"
    f2 = bronze_path+"/exchange_rates.csv"
    f3 = silver_path+"/currency_scd"
    df_codes = spark.read.format("csv").option("header",True).load(f1)
    df_rates = spark.read.format("csv").option("header",True).load(f2)


    df_joined = df_rates.join(
        df_codes,
        on=(upper(df_rates["COUNTRY"]) == df_codes["Entity"]),
        how="inner"
        ).select (
          df_codes.Entity,
          df_codes.AlphabeticCode,
          col("OBS_VALUE").cast(DecimalType(10,6)).alias("ExchangeRate"),
          replace(col("TIME_PERIOD"),lit("M")).alias("TIME_PERIOD")
        )

    f3 = silver_path+"/currency_scd"
    df_joined.write.format("csv").mode("overwrite").option("header",True).save(f3)


build_currency_scd("../bronze","../silver")
