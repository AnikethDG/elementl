"""fct_netsuite_account_running_balance: per-account running balance of posted transaction lines.

Source: netsuite_transactionline (mainline='F') joined to the latest version of netsuite_transaction
(posting='T'). One output row per line, with ``running_balance`` = cumulative netamount per account
ordered by trandate, transaction id, line id.
"""

import logging

from pyspark.sql import Window
from pyspark.sql import functions as F

import udp_silver_common as c

LOG = logging.getLogger("udp_silver.fct_account_running_balance")


def build(spark, args):
    txn = c.read_bronze(spark, "netsuite_transaction")
    txn = c.latest_version(txn, ["id"])
    txn = (txn.filter(c.tf_to_bool("posting"))
              .select(F.col("id").alias("transaction_id"),
                      F.col("tranid"),
                      F.col("type").alias("transaction_type"),
                      c.parse_ns_date("trandate").alias("trandate"),
                      F.col("postingperiod").alias("postingperiod_id"),
                      F.col("entity").alias("entity_id"),
                      F.col("currency"),
                      F.col("subsidiary").alias("subsidiary_id")))

    lines = c.read_bronze(spark, "netsuite_transactionline")
    lines = c.latest_version(lines, ["id"])
    lines = (lines.filter(~F.coalesce(c.tf_to_bool("mainline"), F.lit(False)))
                  .select(F.col("id").alias("line_id"),
                          F.col("transaction").alias("transaction_id"),
                          c.to_int64("linesequencenumber").alias("linesequencenumber"),
                          F.col("account").alias("account_id"),
                          F.col("department").alias("department_id"),
                          F.col("class").alias("class_id"),
                          F.col("location").alias("location_id"),
                          F.col("item").alias("item_id"),
                          c.to_numeric("netamount").alias("netamount"),
                          c.to_numeric("foreignamount").alias("foreignamount"),
                          F.col("memo").alias("line_memo")))

    joined = lines.join(txn, "transaction_id", "inner")

    # deterministic ordering: ids are STRING business keys, order them numerically
    order_cols = [F.col("trandate"),
                  c.to_int64("transaction_id"),
                  c.to_int64("line_id")]
    w_running = (Window.partitionBy("account_id").orderBy(*order_cols)
                 .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    w_seq = Window.partitionBy("account_id").orderBy(*order_cols)

    out = (joined
           .withColumn("running_balance", F.sum("netamount").over(w_running).cast(c.NUMERIC_TYPE))
           .withColumn("account_seq", F.row_number().over(w_seq))
           .select("line_id", "transaction_id", "tranid", "transaction_type", "trandate",
                   "postingperiod_id", "entity_id", "currency", "subsidiary_id",
                   "account_id", "department_id", "class_id", "location_id", "item_id",
                   "linesequencenumber", "netamount", "foreignamount", "running_balance",
                   "account_seq", "line_memo"))
    return out


def main():
    c.configure_logging()
    args = c.parse_args(description=__doc__)
    spark = c.get_spark("udp-silver-fct_account_running_balance")
    out = build(spark, args)
    c.write_silver(out, args.output_table, mode="overwrite")
    c.log_summary(out.orderBy("account_id", "account_seq"), "fct_account_running_balance")
    spark.stop()


if __name__ == "__main__":
    main()
