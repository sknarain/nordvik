from pyspark.sql.functions import col,lit,current_timestamp, expr, substring, trim, round
from pyspark.sql.types import IntegerType, StringType, TimestampType, DecimalType, DoubleType
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
bronze_path="../bronze"
silver_path="../silver"

def clean_crm_accounts():
    f1 = bronze_path+"/crm_accounts.csv"
    f2 = bronze_path+"/erp_customers.csv"
    df_crm = spark.read.format("csv").option("header",True).load(f1) \
            .filter(col("erp_customer_ref").isNull())
    df_crm2 = spark.read.format("csv").option("header",True).load(f1) \
            .filter(col("erp_customer_ref").isNotNull())
 
    df_erp = spark.read.format("csv").option("header",True).load(f2)

    df_joined = df_crm.join(
        df_erp,
        on=(df_crm["country"] == df_erp["country"]) & (df_erp["legal_name"].contains(df_crm["account_name"])),
        how="inner"
        ).select (
          df_crm.account_id,
          df_crm.account_name,
          df_crm.country,
          df_crm.account_owner,
          df_crm.segment,
          df_erp.customer_id.alias("erp_customet_ref"),
          df_crm.created_date.cast(TimestampType()),
          df_crm.status
        ).dropDuplicates(["account_id"])

#df_crm2.union(df_joined).orderBy("account_id").show()
    f3 = silver_path+"/crm_accounts"
    df_crm2.union(df_joined).orderBy("account_id").write.format("csv").mode("overwrite").option("header",True).save(f3)

def clean_erp_customers():
    f1 = bronze_path+"/erp_customers.csv"
    f2 = silver_path+"/erp_customers"
    df_erp = spark.read.format("csv").option("header",True).load(f1)

    df = df_erp.select(
         df_erp.customer_id,
         df_erp.legal_name,
         df_erp.country,
         df_erp.vat_no,
         df_erp.parent_customer_id,
         df_erp.customer_type,
         df_erp.payment_terms,
         df_erp.created_date.cast(TimestampType())
    )

    df.write.format("csv").mode("overwrite").option("header",True).save(f2)

def convert_order_amount_to_EUR():
    f1 = bronze_path+"/erp_orders.csv"
    f2 = silver_path+"/erp_orders"
    f3 = silver_path+"/currency_scd"

    df_orders1 = spark.read.format("csv").option("header",True).load(f1) \
                 .filter(col("currency") != "EUR")
    df_orders2 = spark.read.format("csv").option("header",True).load(f1) \
                 .filter(col("currency") == "EUR")
    df_currency = spark.read.format("csv").option("header",True).load(f3)


    #on=((df_orders1["currency"] == df_currency["AlphabeticCode"]) & (df_orders1["order_date"].contains(df_currency["TIME_PERIOD"]))),
    df_joined = df_orders1.join(
       df_currency,
       on=((df_orders1["currency"] == df_currency["AlphabeticCode"]) & (substring(col("order_date"),1,7) == trim(df_currency["TIME_PERIOD"]))),
       how="left"
       ).select (
         col("order_id"),
         col("order_date").cast(TimestampType()),
         col("billing_customer_id"),
         col("ship_to_site_id"),
         col("ship_to_name"),
         col("country"),
         col("currency"),
         col("line_no"),
         col("item_code"),
         col("description"),
         col("qty").cast(IntegerType()),
         col("unit_price").cast(DecimalType(10,2)),
         col("line_amount").cast(DecimalType(12,2)),
         col("status"),
         col("requested_delivery"),
         df_currency["ExchangeRate"].cast(DecimalType(10,6))
         ).withColumn(
         "Amount_in_EUR",
          (col("line_amount") * col("ExchangeRate")).cast(DecimalType(12,2))
         )
    
    df = df_orders2.select(
         col("order_id"),
         col("order_date").cast(TimestampType()),
         col("billing_customer_id"),
         col("ship_to_site_id"),
         col("ship_to_name"),
         col("country"),
         col("currency"),
         col("line_no"),
         col("item_code"),
         col("description"),
         col("qty").cast(IntegerType()),
         col("unit_price").cast(DecimalType(10,2)),
         col("line_amount").cast(DecimalType(12,2)),
         col("status"),
         col("requested_delivery")
    ).withColumns({
     "ExchangeRate": lit("1.0").cast(DecimalType(10,6)),
     "Amount_in_EUR": col("line_amount").cast(DecimalType(12,2))
    })

    df.union(df_joined).write.format("csv").mode("overwrite").option("header",True).save(f2)

def convert_opportunity_amount_to_EUR():
    f1 = bronze_path+"/crm_opportunities.csv"
    f2 = silver_path+"/crm_opportunities"
    f3 = silver_path+"/currency_scd"

    df_opp1 = spark.read.format("csv").option("header",True).load(f1) \
                 .filter(col("currency") != "EUR")
    df_opp2 = spark.read.format("csv").option("header",True).load(f1) \
                 .filter(col("currency") == "EUR")
    df_currency = spark.read.format("csv").option("header",True).load(f3)


    df_joined = df_opp1.join(
       df_currency,
       on=((df_opp1["currency"] == df_currency["AlphabeticCode"]) & (substring(col("created_date"),1,7) == trim(df_currency["TIME_PERIOD"]))),
       how="left"
       ).select (
         col("opportunity_id"),
         col("account_id"),
         col("opportunity_name"),
         col("stage"),
         col("probability_pct"),
         col("amount").cast(DecimalType(12,2)),
         col("currency"),
         col("expected_close").cast(TimestampType()),
         col("product_family"),
         col("owner"),
         col("created_date").cast(TimestampType()),
         col("closed_reason"),
         col("notes"),
         df_currency["ExchangeRate"].cast(DecimalType(10,6))
         ).withColumn(
         "Amount_in_EUR",
          (col("amount") * col("ExchangeRate")).cast(DecimalType(12,2))
         )
    
    df = df_opp2.select(
         col("opportunity_id"),
         col("account_id"),
         col("opportunity_name"),
         col("stage"),
         col("probability_pct"),
         col("amount").cast(DecimalType(12,2)),
         col("currency"),
         col("expected_close").cast(TimestampType()),
         col("product_family"),
         col("owner"),
         col("created_date").cast(TimestampType()),
         col("closed_reason"),
         col("notes")
    ).withColumns({
     "ExchangeRate": lit("1.0").cast(DecimalType(10,6)),
     "Amount_in_EUR": col("amount").cast(DecimalType(12,2))
    })

    df.union(df_joined).write.format("csv").mode("overwrite").option("header",True).save(f2)

def clean_erp_units():
    f1 = bronze_path+"/erp_units.csv"
    f2 = silver_path+"/erp_units"
    df_erp = spark.read.format("csv").option("header",True).load(f1) 


    df_clean = df_erp.select (
          df_erp.serial_no,
          df_erp.model,
          df_erp.product_family,
          df_erp.plant,
          df_erp.build_date.cast(TimestampType()),
          df_erp.ship_date.cast(TimestampType()),
          df_erp.install_date.cast(TimestampType()),
          df_erp.ship_to_country,
          df_erp.site_name,
          df_erp.site_city,
          df_erp.billing_customer_id,
          df_erp.order_id,
          df_erp.warranty_end.cast(TimestampType()),
          df_erp.config
        )

    df_clean.write.format("csv").mode("overwrite").option("header",True).save(f2)

def clean_service_contracts():
    f1 = bronze_path+"/service_contracts.csv"
    f2 = silver_path+"/service_contracts"
    df_sc = spark.read.format("csv").option("header",True).load(f1) 


    df_clean = df_sc.select (
          df_sc.contract_id,
          df_sc.billing_customer_id,
          df_sc.customer_name,
          df_sc.country,
          df_sc.contract_type,
          df_sc.start_date.cast(TimestampType()),
          df_sc.end_date.cast(TimestampType()),
          df_sc.previous_contract_id,
          df_sc.currency,
          df_sc.annual_fee.cast(DecimalType(12,2)),
          df_sc.sla_response_hours.cast(IntegerType()),
          df_sc.target_margin_pct.cast(IntegerType()),
          df_sc.billed_12m_eur.cast(DecimalType(12,2)),
          df_sc.cost_to_serve_12m_eur.cast(DecimalType(12,2)),
          df_sc.covered_units_count.cast(IntegerType()),
          df_sc.covered_serials
        ).withColumn(
         "margin_pct",
          round((col("billed_12m_eur") - col("cost_to_serve_12m_eur"))*100/col("billed_12m_eur"),0)
         )

    df_clean.write.format("csv").mode("overwrite").option("header",True).save(f2)

def clean_service_tickets():
    f1 = bronze_path+"/service_tickets.csv"
    f2 = silver_path+"/service_tickets"
    f3 = silver_path+"/service_contracts"
    df_sr1 = spark.read.format("csv").option("header",True).load(f1) \
             .filter(col("contract_ref").isNull())
    df_sr2 = spark.read.format("csv").option("header",True).load(f1) \
             .filter(col("contract_ref").isNotNull())
    df_sc = spark.read.format("csv").option("header",True).load(f3)


    df_joined = df_sr1.join(
           df_sc,
           on = (trim(df_sr1["customer_name"]) == trim(df_sc["customer_name"])),
           how = "inner"
          ).select (
           df_sr1.ticket_id,
           df_sr1.opened_at.cast(TimestampType()),
           df_sr1.closed_at.cast(TimestampType()),
           df_sr1.status,
           df_sr1.customer_name,
           df_sr1.site_name,
           df_sr1.country,
           df_sr1.serial_no,
           df_sr1.model_reported,
           df_sr1.category,
           df_sr1.symptom,
           df_sr1.activity_code,
           df_sr1.technician,
           df_sr1.labour_hours.cast(DecimalType(4,2)),
           df_sr1.travel_hours.cast(DecimalType(4,2)),
           df_sr1.labour_cost_eur.cast(DecimalType(6,2)),
           df_sr1.travel_cost_eur.cast(DecimalType(6,2)),
           df_sr1.parts_replaced,
           df_sr1.parts_cost_eur.cast(DecimalType(6,2)),
           df_sr1.billing_type,
           df_sr1.cost_centre,
           df_sc.contract_id,
           df_sr1.notes
        )

    df_sr = df_sr2.select(
           df_sr2.ticket_id,
           df_sr2.opened_at.cast(TimestampType()),
           df_sr2.closed_at.cast(TimestampType()),
           df_sr2.status,
           df_sr2.customer_name,
           df_sr2.site_name,
           df_sr2.country,
           df_sr2.serial_no,
           df_sr2.model_reported,
           df_sr2.category,
           df_sr2.symptom,
           df_sr2.activity_code,
           df_sr2.technician,
           df_sr2.labour_hours.cast(DecimalType(4,2)),
           df_sr2.travel_hours.cast(DecimalType(4,2)),
           df_sr2.labour_cost_eur.cast(DecimalType(6,2)),
           df_sr2.travel_cost_eur.cast(DecimalType(6,2)),
           df_sr2.parts_replaced,
           df_sr2.parts_cost_eur.cast(DecimalType(6,2)),
           df_sr2.billing_type,
           df_sr2.cost_centre,
           df_sr2.contract_ref,
           df_sr2.notes
    )

    df_joined.union(df_sr2).write.format("csv").mode("overwrite").option("header",True).save(f2)

clean_service_tickets()
clean_service_contracts()
clean_erp_units()
convert_opportunity_amount_to_EUR()
convert_order_amount_to_EUR()
clean_erp_customers()
clean_crm_accounts()
