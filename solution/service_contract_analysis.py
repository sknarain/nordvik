from pyspark.sql.functions import col,trim, sum, count, round
from pyspark.sql.types import IntegerType, StringType, TimestampType, DecimalType, DoubleType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
silver_path="../silver"
gold_path="../gold"

f1 = silver_path+"/service_contracts"
f2 = silver_path+"/service_tickets"
f3 = gold_path+"/below_margin_contracts"

df_sc = spark.read.format("csv").option("header",True).load(f1) \
        .filter(col("margin_pct") < col("target_margin_pct"))
df_sr = spark.read.format("csv").option("header",True).load(f2)

df = df_sc.join(
     df_sr,
     on=(df_sc.contract_id == df_sr.contract_id),
     how="inner"
     ).select(
       df_sc.contract_id,
       df_sr.ticket_id,
       df_sr.category,
       df_sr.labour_cost_eur,
       df_sr.travel_cost_eur,
       df_sr.parts_cost_eur
     )
 

df1 = df.groupby(df.contract_id).agg(round(sum("labour_cost_eur"),2).alias("Total_Labour_Cost"))
df2 = df.groupby(df.contract_id).agg(round(sum("travel_cost_eur"),2).alias("Total_Travel_Cost"))
df3 = df.groupby(df.contract_id).agg(round(sum("parts_cost_eur"),2).alias("Total_Parts_Cost"))
df4 = df.filter(col("category") == "Breakdown").groupby(df.contract_id).agg(count("*").alias("Repeated_Visits"))

df1 = df1.withColumnRenamed("contract_id","df1_contract_id")
df2 = df2.withColumnRenamed("contract_id","df2_contract_id")
df3 = df3.withColumnRenamed("contract_id","df3_contract_id")
df4 = df4.withColumnRenamed("contract_id","df4_contract_id")

df1 = df1.alias("a")
df2 = df2.alias("b")
df3 = df3.alias("c")
df4 = df4.alias("d")

df = df1.join(
     df2,
     on = (df1["df1_contract_id"]  == df2["df2_contract_id"]),
     how = "inner"
     ).join(
     df3,
     on = (df1["df1_contract_id"] == df3["df3_contract_id"]),
     how = "inner"
     ).join(
     df4,
     on = (df1["df1_contract_id"] == df4["df4_contract_id"]),
     how = "inner"
     ).select(
      col("df1_contract_id").alias("Contract_Id"),
      "a.Total_Labour_Cost",
      "b.Total_Travel_Cost",
      "c.Total_Parts_Cost",
      "d.Repeated_Visits"
     )

df.write.format("csv").mode("overwrite").option("header",True).save(f3)      
#df.show()
