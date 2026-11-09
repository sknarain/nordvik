from pyspark.sql.functions import col,trim, sum, count, round
from pyspark.sql.types import IntegerType, StringType, TimestampType, DecimalType, DoubleType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
silver_path="../silver"
gold_path="../gold"

f1 = silver_path+"/erp_orders"
f2 = silver_path+"/erp_customers"
f3 = silver_path+"/crm_opportunities"
f4 = silver_path+"/service_tickets"
f5 = silver_path+"/crm_accounts"
f6 = gold_path+"/aggregates"

df_orders = spark.read.format("csv").option("header",True).load(f1)
df_customers = spark.read.format("csv").option("header",True).load(f2)
df_opps = spark.read.format("csv").option("header",True).load(f3)
df_tickets = spark.read.format("csv").option("header",True).load(f4)
df_accounts = spark.read.format("csv").option("header",True).load(f5)

df = df_orders.join(
     df_customers,
     on=(df_orders.billing_customer_id == df_customers.customer_id),
     how="inner"
     ).select(
       df_customers.legal_name.alias("CustomerName"),
       col("Amount_in_EUR")
     )

df1 = df.groupby(df.CustomerName).agg(round(sum("Amount_in_EUR"),2).alias("Total_Revenue"))

df = df_opps.join(
     df_accounts,
     on=(df_opps.account_id == df_accounts.account_id),
     how="inner"
     ).join(
     df_customers,
     on=(df_customers.customer_id == df_accounts.erp_customer_ref),
     how="inner"
     ).select(
       df_customers.legal_name.alias("CustomerName"),
       col("Amount_in_EUR")
     )

df2 = df.groupby(df.CustomerName).agg(round(sum("Amount_in_EUR"),2).alias("Open_Opportunities"))

df3 = df_tickets.filter(col("status") != "Closed").groupby(df_tickets.customer_name.alias("CustomerName")).agg(count("*").alias("Open_Tickets"))

df = df1.join(
     df2,
     on = (trim(df1["CustomerName"]) == trim(df2["CustomerName"])),
     how="inner"
     ).join(
     df3,
     on = (trim(df1["CustomerName"]) == trim(df3["CustomerName"])),
     how="inner"
     ).select(
      df1["CustomerName"],
      df1["Total_Revenue"],
      df2["Open_Opportunities"],
      df3["Open_Tickets"]
    )
df.write.format("csv").mode("overwrite").option("header",True).save(f6)      
#df.show()
