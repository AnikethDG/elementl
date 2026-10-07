"""dim_netsuite_classification_path: flattened classification parent hierarchy.

Iterative self-joins (max 10 levels, stops when no new rows resolve). Columns: id, name, parent_id,
root_id, level (1 = root), path ('Class 1 > Class 4 > Class 13'), id_path ('1/4/13'), isinactive.
Nodes whose parent chain cannot be resolved within max_levels (orphans / cycles) are still emitted
with level/root_id/path NULL so the key stays complete; a warning is logged.
"""

import logging

from pyspark.sql import functions as F

import udp_silver_common as c

LOG = logging.getLogger("udp_silver.dim_classification_path")

PATH_SEP = " > "
ID_SEP = "/"


def flatten_hierarchy(nodes, max_levels=10):
    """nodes: DataFrame(id, name, parent_id). Returns (resolved, unresolved) DataFrames."""
    nodes = nodes.cache()
    level1 = (nodes.filter(F.col("parent_id").isNull())
                   .select("id", "name", "parent_id",
                           F.col("id").alias("root_id"),
                           F.lit(1).alias("level"),
                           F.col("name").alias("path"),
                           F.col("id").alias("id_path")))
    resolved = level1.cache()
    frontier = level1
    for lvl in range(2, max_levels + 1):
        parent = frontier.select(F.col("id").alias("p_id"), F.col("root_id").alias("p_root"),
                                 F.col("path").alias("p_path"), F.col("id_path").alias("p_id_path"))
        nxt = (nodes.join(parent, nodes.parent_id == parent.p_id, "inner")
                    .select("id", "name", "parent_id",
                            F.col("p_root").alias("root_id"),
                            F.lit(lvl).alias("level"),
                            F.concat(F.col("p_path"), F.lit(PATH_SEP), F.col("name")).alias("path"),
                            F.concat(F.col("p_id_path"), F.lit(ID_SEP), F.col("id")).alias("id_path"))
                    .cache())
        n = nxt.count()
        LOG.info("level %d: %d nodes", lvl, n)
        if n == 0:
            break
        resolved = resolved.unionByName(nxt).cache()
        frontier = nxt
    unresolved = nodes.join(resolved.select("id"), "id", "left_anti")
    return resolved, unresolved


def build(spark, args):
    max_levels = int(args.params.get("max_levels", 10))
    cls = c.latest_version(c.read_bronze(spark, "netsuite_classification"), ["id"])
    nodes = cls.select(F.col("id"),
                       F.trim(F.col("name")).alias("name"),
                       F.when(F.trim(F.col("parent")) == "", None).otherwise(F.trim(F.col("parent"))).alias("parent_id"))

    resolved, unresolved = flatten_hierarchy(nodes, max_levels)
    n_unresolved = unresolved.count()
    if n_unresolved:
        LOG.warning("%d classification nodes could not be resolved within %d levels (orphan/cycle)",
                    n_unresolved, max_levels)
    unresolved = unresolved.select("id", "name", "parent_id",
                                   F.lit(None).cast("string").alias("root_id"),
                                   F.lit(None).cast("int").alias("level"),
                                   F.lit(None).cast("string").alias("path"),
                                   F.lit(None).cast("string").alias("id_path"))
    tree = resolved.unionByName(unresolved)

    attrs = cls.select(F.col("id"),
                       F.col("fullname"),
                       c.tf_to_bool("isinactive").alias("isinactive"),
                       c.parse_ns_timestamp("lastmodifieddate").alias("lastmodifieddate"))
    parents = (nodes.filter(F.col("parent_id").isNotNull())
                    .select(F.col("parent_id").alias("id")).distinct()
                    .withColumn("has_children", F.lit(True)))
    out = (tree.join(attrs, "id", "left")
               .join(parents, "id", "left")
              .withColumn("is_leaf", ~F.coalesce(F.col("has_children"), F.lit(False)))
              .select("id", "name", "parent_id", "root_id", "level", "path", "id_path",
                      "is_leaf", "fullname", "isinactive", "lastmodifieddate"))
    return out


def main():
    c.configure_logging()
    args = c.parse_args(description=__doc__)
    spark = c.get_spark("udp-silver-dim_classification_path")
    out = build(spark, args)
    c.write_silver(out, args.output_table, mode="overwrite")
    c.log_summary(out.orderBy(F.col("level").desc_nulls_last(), "id"), "dim_classification_path")
    spark.stop()


if __name__ == "__main__":
    main()
