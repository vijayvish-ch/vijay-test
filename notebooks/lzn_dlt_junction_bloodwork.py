# Databricks notebook source
# =============================================================================
# NOTEBOOK 1 OF 2 — LANDING ZONE (LZ)
# Target: dev_cascaid.lzn_junction
# Tables: lzn_manifest | lzn_orders | lzn_results
#
# Purpose: Ingest files AS-IS from Databricks Volume into Delta tables.
#          No transformations. One row = one source record.
#          New batch folder drops trigger Auto Loader automatically.
#
# =============================================================================

import dlt
from pyspark.sql.functions import col, current_timestamp, regexp_extract

# ---------------------------------------------------------------------------
# VOLUME SOURCE PATH
# New folders land here after every API run:
#   /Volumes/dev_cascaid/lzn_junction/junction_bloodwork/
#       └── 20260224T002244_a3adadfa-.../
#               ├── manifest.json
#               ├── orders/all_orders.json
#               └── results/result_*.json
# ---------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# PIPELINE PARAMETERS
# -----------------------------------------------------------------------------
CATALOG = spark.conf.get("catalog")
SCHEMA = spark.conf.get("schema")
VOLUME_NAME = spark.conf.get("volume_name")
ENV = spark.conf.get("env")
SOURCE_FOLDER = spark.conf.get("source_folder", "junction_bloodwork")

# -----------------------------------------------------------------------------
# PATHS
# -----------------------------------------------------------------------------
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{SOURCE_FOLDER}"
BATCH_REGEX = rf".*/{SOURCE_FOLDER}/([^/]+)/.*"


#BASE_PATH   = "/Volumes/dev_cascaid/lzn_junction_bloodwork/junction_bloodwork"

# Extracts the batch folder name from the file path
# e.g. "20260224T002244_a3adadfa-25fe-427d-b82e-dabfae5cf466"
#BATCH_REGEX = r".*/junction_bloodwork/([^/]+)/.*"


# =============================================================================
# TABLE 1: lzn_manifest
# One row per manifest.json (one per API batch run)
# Kept completely raw — no transformations
# =============================================================================
@dlt.table(
    name    = "manifests",
    comment = "LZ: Raw manifest.json — one row per API batch run. No transformations.",
    table_properties = {
        "quality": "landing",
        "pipelines.autoOptimize.managed": "true",
    }
)
def lzn_manifest():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("multiLine", "true")
            .option("pathGlobFilter", "manifest.json")
            .load(BASE_PATH)
            # _metadata.file_path is the UC-approved replacement for input_file_name()
            .withColumn("_source_file",  col("_metadata.file_path"))
            .withColumn("_batch_folder", regexp_extract(col("_metadata.file_path"), BATCH_REGEX, 1))
            .withColumn("_ingested_at",  current_timestamp())
    )


# =============================================================================
# TABLE 2: lzn_orders
# One row per all_orders.json (one per API batch run)
# Full orders array stored as-is — NOT exploded here
# =============================================================================
@dlt.table(
    name    = "orders",
    comment = "LZ: Raw all_orders.json — one row per batch file. Orders array not exploded.",
    table_properties = {
        "quality": "landing",
        "pipelines.autoOptimize.managed": "true",
    }
)
def lzn_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("multiLine", "true")
            .option("pathGlobFilter", "all_orders.json")
            .load(BASE_PATH)
            .withColumn("_source_file",  col("_metadata.file_path"))
            .withColumn("_batch_folder", regexp_extract(col("_metadata.file_path"), BATCH_REGEX, 1))
            .withColumn("_ingested_at",  current_timestamp())
    )


# =============================================================================
# TABLE 3: lzn_results
# One row per result_*.json file
# Full results array stored as-is — NOT exploded here
# =============================================================================
@dlt.table(
    name    = "results",
    comment = "LZ: Raw result_*.json — one row per result file. Results array not exploded.",
    table_properties = {
        "quality": "landing",
        "pipelines.autoOptimize.managed": "true",
    }
)
def lzn_results():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("multiLine", "true")
            .option("pathGlobFilter", "result_*.json")
            .load(BASE_PATH)
            .withColumn("_source_file",  col("_metadata.file_path"))
            .withColumn("_batch_folder", regexp_extract(col("_metadata.file_path"), BATCH_REGEX, 1))
            .withColumn("_ingested_at",  current_timestamp())
    )