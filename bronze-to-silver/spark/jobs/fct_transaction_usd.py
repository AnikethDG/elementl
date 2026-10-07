"""fct_netsuite_transaction_usd: latest transaction versions with an as-of FX conversion to USD.

For every transaction the USD-base rate with the greatest ``effectivedate <= trandate`` for its
currency is picked (USD itself => rate 1.0, rate_effective_date NULL).
amount_usd = foreigntotal * rate. Key: transaction_id.
"""

import logging

from pyspark.sql import Window
from pyspark.sql import functions as F

import udp_silver_common as c

LOG = logging.getLogger("udp_silver.fct_transaction_usd")

RATE_TYPE = "decimal(20,10)"


def build(spark, args):
    base_currency = str(args.params.get("base_currency", "USD")).upper()

    txn = c.latest_version(c.read_bronze(spark, "netsuite_transaction"), ["id"])
    txn = txn.select(F.col("id").alias("transaction_id"),
                     F.col("tranid"),
                     F.col("type").alias("transaction_type"),
                     c.parse_ns_date("trandate").alias("trandate"),
                     F.col("postingperiod").alias("postingperiod_id"),
                     c.tf_to_bool("posting").alias("posting"),
                     F.col("entity").alias("entity_id"),
                     F.upper(F.trim(F.col("currency"))).alias("currency"),
                     F.col("subsidiary").alias("subsidiary_id"),
                     c.to_numeric("foreigntotal").alias("foreigntotal"),
                     F.col("status"),
                     F.col("memo"),
                     c.parse_ns_timestamp("lastmodifieddate").alias("lastmodifieddate"),
                     F.col("ingestion_date"),
                     F.col("ingestion_timestamp"))

    rates = c.read_bronze(spark, "netsuite_currencyrate")
    rates = (rates.filter(F.upper(F.trim(F.col("basecurrency"))) == base_currency)
                  .select(F.upper(F.trim(F.col("transactioncurrency"))).alias("currency"),
                          F.trim(F.col("exchangerate")).cast(RATE_TYPE).alias("rate"),
                          c.parse_ns_date("effectivedate").alias("rate_effective_date"),
                          F.col("ingestion_timestamp").alias("_rate_ingested_at"))
                  .filter(F.col("rate").isNotNull() & F.col("rate_effective_date").isNotNull()))
    # a rate table may hold several versions of the same (currency, effectivedate): latest wins
    rates = c.latest_version(rates, ["currency", "rate_effective_date"], ts_col="_rate_ingested_at") \
             .drop("_rate_ingested_at")

    # as-of join: broadcast the small rate table, keep the latest rate on/before trandate
    candidates = txn.join(F.broadcast(rates),
                          (txn.currency == rates.currency)
                          & (rates.rate_effective_date <= txn.trandate),
                          "left").drop(rates.currency)
    w = Window.partitionBy("transaction_id").orderBy(F.col("rate_effective_date").desc_nulls_last())
    picked = (candidates.withColumn("_rn", F.row_number().over(w))
                        .filter(F.col("_rn") == 1).drop("_rn"))

    is_base = F.col("currency") == F.lit(base_currency)
    rate_final = F.when(is_base, F.lit(1).cast(RATE_TYPE)).otherwise(F.col("rate"))
    out = (picked
           .withColumn("rate", rate_final)
           .withColumn("rate_effective_date",
                       F.when(is_base, F.lit(None).cast("date")).otherwise(F.col("rate_effective_date")))
           .withColumn("rate_source",
                       F.when(is_base, F.lit("base"))
                        .when(F.col("rate").isNotNull(), F.lit("asof"))
                        .otherwise(F.lit("missing")))
           .withColumn("amount_usd",
                       F.round(F.col("foreigntotal") * F.col("rate"), 6).cast(c.NUMERIC_TYPE))
           .withColumn("rate", F.col("rate").cast(c.NUMERIC_TYPE))
           .withColumn("base_currency", F.lit(base_currency))
           .select("transaction_id", "tranid", "transaction_type", "trandate", "postingperiod_id",
                   "posting", "entity_id", "subsidiary_id", "status", "memo",
                   "currency", "base_currency", "foreigntotal", "rate", "rate_effective_date",
                   "rate_source", "amount_usd", "lastmodifieddate",
                   "ingestion_date", "ingestion_timestamp"))
    return out


def main():
    c.configure_logging()
    args = c.parse_args(description=__doc__)
    spark = c.get_spark("udp-silver-fct_transaction_usd")
    out = build(spark, args)
    c.write_silver(out, args.output_table, mode="overwrite")
    c.log_summary(out.filter(F.col("currency") != "USD"), "fct_transaction_usd (non-USD)")
    spark.stop()


if __name__ == "__main__":
    main()
