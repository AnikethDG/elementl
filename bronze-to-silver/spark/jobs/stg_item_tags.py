"""stg_netsuite_item_tags: one row per (item id, #hashtag) extracted from the item description.

Items with no hashtags are kept as a single row with ``tag`` NULL. ``displayname`` is normalised
(trim, collapse whitespace, title case). Key: (item_id, tag).
"""

import logging

from pyspark.sql import functions as F

import udp_silver_common as c

LOG = logging.getLogger("udp_silver.stg_item_tags")


def build(spark, args):
    items = c.latest_version(c.read_bronze(spark, "netsuite_item"), ["id"])

    tags = c.extract_hashtags("description")
    # keep tag-less items as one row with NULL tag
    tags_or_null = F.when(F.size(tags) == 0, F.array(F.lit(None).cast("string"))).otherwise(tags)

    out = (items
           .withColumn("tag_array", tags_or_null)
           .withColumn("tag_count", F.size(tags))
           .select(F.col("id").alias("item_id"),
                   F.col("itemid").alias("item_code"),
                   c.normalise_name("displayname").alias("displayname"),
                   F.col("description"),
                   F.col("itemtype"),
                   c.to_numeric("baseprice").alias("baseprice"),
                   c.tf_to_bool("isinactive").alias("isinactive"),
                   c.parse_ns_timestamp("lastmodifieddate").alias("lastmodifieddate"),
                   F.col("tag_count"),
                   F.posexplode("tag_array").alias("tag_position", "tag"))
           .withColumn("tag", F.lower(F.col("tag")))
           .withColumn("tag_position", F.when(F.col("tag").isNull(), F.lit(None).cast("int"))
                                        .otherwise(F.col("tag_position") + 1))
           .dropDuplicates(["item_id", "tag"]))  # same tag twice in one description -> one row
    return out


def main():
    c.configure_logging()
    args = c.parse_args(description=__doc__)
    spark = c.get_spark("udp-silver-stg_item_tags")
    out = build(spark, args)
    c.write_silver(out, args.output_table, mode="overwrite")
    c.log_summary(out, "stg_item_tags")
    spark.stop()


if __name__ == "__main__":
    main()
