"""stg_netsuite_employee: PII-masked, typed employee staging table (one row per employee id).

* email   -> sha256 hex of lower(trim(email))
* phone   -> last 4 digits only, as '***-***-1234'
* firstname/lastname dropped, ``initials`` kept
* hiredate 'M/D/YYYY' -> DATE, ``tenure_days`` = days between hiredate and --ds
"""

import logging

from pyspark.sql import functions as F

import udp_silver_common as c

LOG = logging.getLogger("udp_silver.stg_employee_masked")


def build(spark, args):
    emp = c.latest_version(c.read_bronze(spark, "netsuite_employee"), ["id"])
    as_of = F.to_date(F.lit(args.ds))
    hiredate = c.parse_ns_date("hiredate")

    out = emp.select(
        F.col("id").alias("employee_id"),
        F.col("entityid"),
        F.concat(F.upper(F.substring(F.trim(F.coalesce(F.col("firstname"), F.lit(""))), 1, 1)),
                 F.upper(F.substring(F.trim(F.coalesce(F.col("lastname"), F.lit(""))), 1, 1))).alias("initials"),
        c.sha256_hex("email").alias("email_sha256"),
        F.regexp_extract(F.lower(F.trim(F.col("email"))), r"@(.+)$", 1).alias("email_domain"),
        c.mask_phone("phone").alias("phone_masked"),
        hiredate.alias("hiredate"),
        F.datediff(as_of, hiredate).alias("tenure_days"),
        as_of.alias("tenure_as_of"),
        F.col("title"),
        F.col("department").alias("department_id"),
        F.col("supervisor").alias("supervisor_id"),
        c.tf_to_bool("isinactive").alias("isinactive"),
        c.parse_ns_timestamp("lastmodifieddate").alias("lastmodifieddate"),
        F.col("ingestion_date"),
        F.col("ingestion_timestamp"),
    ).withColumn("initials", F.when(F.col("initials") == "", F.lit(None)).otherwise(F.col("initials")))
    return out


def main():
    c.configure_logging()
    args = c.parse_args(description=__doc__)
    spark = c.get_spark("udp-silver-stg_employee_masked")
    out = build(spark, args)
    c.write_silver(out, args.output_table, mode="overwrite")
    c.log_summary(out, "stg_employee_masked")
    spark.stop()


if __name__ == "__main__":
    main()
