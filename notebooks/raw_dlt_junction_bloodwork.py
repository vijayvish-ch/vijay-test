# Databricks notebook source
# =============================================================================
# NOTEBOOK 2 OF 2 — RAW ZONE (FLATTENED)
# Pipeline : ch-dlt-pipeline-junction-bloodwork-RZ
# Target   : dev_cascaid.lzn_junction_rz   (set in pipeline_rz.json)
# Source   : dev_cascaid.lzn_junction_lz   (separate LZ pipeline)
#
# IMPORTANT — CROSS-SCHEMA READS:
#   This notebook is in its OWN pipeline whose schema is lzn_junction_rz.
#   It reads LZ tables from a DIFFERENT schema (lzn_junction_lz).
#   Use dlt.read_stream("dev_cascaid.lzn_junction_lz.lzn_orders") — fully
#   qualified 3-part name — whenever reading from the LZ pipeline output.
#
# ROOT CAUSE OF ORIGINAL ISSUE:
#   Both notebooks were in one pipeline sharing schema = test_vijay_schema,
#   so LZ and RZ tables landed in the same schema. Solution: split into two
#   pipelines with different target schemas and use fully-qualified table
#   names to read across schemas.
# =============================================================================

import dlt
from pyspark.sql.functions import (
    col, explode, explode_outer,
    to_timestamp, from_json,
    current_timestamp
)
from pyspark.sql.types import *

# =============================================================================
# CROSS-SCHEMA SOURCE REFERENCES
# LZ tables live in dev_cascaid.lzn_junction_lz (the LZ pipeline's schema).
# Always use the fully-qualified 3-part name when reading across pipelines.
# =============================================================================
LZN_CATALOG = spark.conf.get("catalog")
LZN_SCHEMA = spark.conf.get("source_schema", "lzn_junction_bloodwork")
ENV = spark.conf.get("env")
#LZN_CATALOG = "dev_cascaid"
#LZN_SCHEMA  = "lzn_junction_bloodwork"

LZN_MANIFEST = f"{LZN_CATALOG}.{LZN_SCHEMA}.manifests"
LZN_ORDERS   = f"{LZN_CATALOG}.{LZN_SCHEMA}.orders"
LZN_RESULTS  = f"{LZN_CATALOG}.{LZN_SCHEMA}.results"


# =============================================================================
# SCHEMAS — explicit StructType so from_json() can parse LZ string columns
# (LZ used inferColumnTypes=false — everything stored as STRING)
# =============================================================================

ORDER_TXN_CHILD_SCHEMA = StructType([
    StructField("id",                          StringType(),  True),
    StructField("low_level_status",            StringType(),  True),
    StructField("low_level_status_created_at", StringType(),  True),
    StructField("origin",                      StringType(),  True),
    StructField("parent_id",                   StringType(),  True),
    StructField("created_at",                  StringType(),  True),
    StructField("updated_at",                  StringType(),  True),
])

ORDER_TXN_SCHEMA = StructType([
    StructField("id",     StringType(), True),
    StructField("status", StringType(), True),
    StructField("orders", ArrayType(ORDER_TXN_CHILD_SCHEMA), True),
])

MARKER_SCHEMA = StructType([
    StructField("id",                  LongType(),    True),
    StructField("name",                StringType(),  True),
    StructField("slug",                StringType(),  True),
    StructField("description",         StringType(),  True),
    StructField("lab_id",              IntegerType(), True),
    StructField("provider_id",         StringType(),  True),
    StructField("type",                StringType(),  True),
    StructField("unit",                StringType(),  True),
    StructField("price",               StringType(),  True),
    StructField("aoe",                 StringType(),  True),
    StructField("a_la_carte_enabled",  BooleanType(), True),
    StructField("common_tat_days",     IntegerType(), True),
    StructField("worst_case_tat_days", IntegerType(), True),
    StructField("is_orderable",        BooleanType(), True),
])

LAB_SCHEMA = StructType([
    StructField("id",                 IntegerType(), True),
    StructField("slug",               StringType(),  True),
    StructField("name",               StringType(),  True),
    StructField("first_line_address", StringType(),  True),
    StructField("city",               StringType(),  True),
    StructField("zipcode",            StringType(),  True),
    StructField("collection_methods", ArrayType(StringType()), True),
    StructField("sample_types",       ArrayType(StringType()), True),
])

LAB_TEST_SCHEMA = StructType([
    StructField("id",                          StringType(),             True),
    StructField("slug",                        StringType(),             True),
    StructField("name",                        StringType(),             True),
    StructField("sample_type",                 StringType(),             True),
    StructField("method",                      StringType(),             True),
    StructField("price",                       DoubleType(),             True),
    StructField("is_active",                   BooleanType(),            True),
    StructField("status",                      StringType(),             True),
    StructField("fasting",                     BooleanType(),            True),
    StructField("is_delegated",                BooleanType(),            True),
    StructField("auto_generated",              BooleanType(),            True),
    StructField("has_collection_instructions", BooleanType(),            True),
    StructField("common_tat_days",             IntegerType(),            True),
    StructField("worst_case_tat_days",         IntegerType(),            True),
    StructField("lab",                         LAB_SCHEMA,               True),
    StructField("markers",                     ArrayType(MARKER_SCHEMA), True),
])

PATIENT_DETAILS_SCHEMA = StructType([
    StructField("first_name",         StringType(), True),
    StructField("last_name",          StringType(), True),
    StructField("dob",                StringType(), True),
    StructField("gender",             StringType(), True),
    StructField("phone_number",       StringType(), True),
    StructField("email",              StringType(), True),
    StructField("medical_proxy",      StringType(), True),
    StructField("race",               StringType(), True),
    StructField("ethnicity",          StringType(), True),
    StructField("sexual_orientation", StringType(), True),
    StructField("gender_identity",    StringType(), True),
])

PATIENT_ADDRESS_SCHEMA = StructType([
    StructField("receiver_name", StringType(), True),
    StructField("first_line",    StringType(), True),
    StructField("second_line",   StringType(), True),
    StructField("city",          StringType(), True),
    StructField("state",         StringType(), True),
    StructField("zip",           StringType(), True),
    StructField("country",       StringType(), True),
    StructField("phone_number",  StringType(), True),
])

PHYSICIAN_SCHEMA = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name",  StringType(), True),
    StructField("npi",        StringType(), True),
])

DETAILS_DATA_SCHEMA = StructType([
    StructField("id",             StringType(), True),
    StructField("created_at",     StringType(), True),
    StructField("updated_at",     StringType(), True),
    StructField("appointment_id", StringType(), True),
])

DETAILS_SCHEMA = StructType([
    StructField("type", StringType(),        True),
    StructField("data", DETAILS_DATA_SCHEMA, True),
])

EVENT_SCHEMA = StructType([
    StructField("id",         LongType(),   True),
    StructField("created_at", StringType(), True),
    StructField("status",     StringType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("id",                        StringType(),              True),
    StructField("user_id",                   StringType(),              True),
    StructField("team_id",                   StringType(),              True),
    StructField("status",                    StringType(),              True),
    StructField("origin",                    StringType(),              True),
    StructField("priority",                  BooleanType(),             True),
    StructField("billing_type",              StringType(),              True),
    StructField("has_abn",                   BooleanType(),             True),
    StructField("interpretation",            StringType(),              True),
    StructField("has_missing_results",       BooleanType(),             True),
    StructField("icd_codes",                 StringType(),              True),
    StructField("sample_id",                 StringType(),              True),
    StructField("notes",                     StringType(),              True),
    StructField("health_insurance_id",       StringType(),              True),
    StructField("passthrough",               StringType(),              True),
    StructField("requisition_form_url",      StringType(),              True),
    StructField("created_at",                StringType(),              True),
    StructField("updated_at",                StringType(),              True),
    StructField("activate_by",               StringType(),              True),
    StructField("expected_result_by_date",   StringType(),              True),
    StructField("worst_case_result_by_date", StringType(),              True),
    StructField("patient_details",           PATIENT_DETAILS_SCHEMA,    True),
    StructField("patient_address",           PATIENT_ADDRESS_SCHEMA,    True),
    StructField("lab_test",                  LAB_TEST_SCHEMA,           True),
    StructField("physician",                 PHYSICIAN_SCHEMA,          True),
    StructField("details",                   DETAILS_SCHEMA,            True),
    StructField("events",                    ArrayType(EVENT_SCHEMA),   True),
    StructField("order_transaction",         ORDER_TXN_SCHEMA,          True),
])

SOURCE_MARKER_SCHEMA = StructType([
    StructField("marker_id",   LongType(),   True),
    StructField("name",        StringType(), True),
    StructField("slug",        StringType(), True),
    StructField("provider_id", StringType(), True),
])

RESULT_ITEM_SCHEMA = StructType([
    StructField("name",                 StringType(),                    True),
    StructField("slug",                 StringType(),                    True),
    StructField("value",                DoubleType(),                    True),
    StructField("result",               StringType(),                    True),
    StructField("type",                 StringType(),                    True),
    StructField("unit",                 StringType(),                    True),
    StructField("timestamp",            StringType(),                    True),
    StructField("notes",                StringType(),                    True),
    StructField("reference_range",      StringType(),                    True),
    StructField("min_range_value",      DoubleType(),                    True),
    StructField("max_range_value",      DoubleType(),                    True),
    StructField("is_above_max_range",   BooleanType(),                   True),
    StructField("is_below_min_range",   BooleanType(),                   True),
    StructField("interpretation",       StringType(),                    True),
    StructField("loinc",                StringType(),                    True),
    StructField("loinc_slug",           StringType(),                    True),
    StructField("provider_id",          StringType(),                    True),
    StructField("performing_laboratory",StringType(),                    True),
    StructField("source_sample_id",     StringType(),                    True),
    StructField("source_markers",       ArrayType(SOURCE_MARKER_SCHEMA), True),
])

METADATA_SCHEMA = StructType([
    StructField("account_id",      StringType(), True),
    StructField("age",             StringType(), True),
    StructField("clia_#",          StringType(), True),
    StructField("date_collected",  StringType(), True),
    StructField("date_received",   StringType(), True),
    StructField("date_reported",   StringType(), True),
    StructField("dob",             StringType(), True),
    StructField("interpretation",  StringType(), True),
    StructField("laboratory",      StringType(), True),
    StructField("patient",         StringType(), True),
    StructField("patient_id",      StringType(), True),
    StructField("provider",        StringType(), True),
    StructField("specimen_number", StringType(), True),
    StructField("status",          StringType(), True),
])

MANIFEST_JOB_SCHEMA = StructType([
    StructField("type",       StringType(), True),
    StructField("folderName", StringType(), True),
    StructField("startTime",  StringType(), True),
    StructField("endTime",    StringType(), True),
    StructField("durationMs", LongType(),   True),
])

MANIFEST_SUMMARY_SCHEMA = StructType([
    StructField("ordersProcessed",    IntegerType(), True),
    StructField("resultsProcessed",   IntegerType(), True),
    StructField("errorsCount",        IntegerType(), True),
    StructField("totalFileSize",      LongType(),    True),
    StructField("totalFileSizeHuman", StringType(),  True),
])

MANIFEST_FILES_SCHEMA = StructType([
    StructField("ordersFile", StructType([
        StructField("location",  StringType(), True),
        StructField("sizeBytes", LongType(),   True),
    ]), True),
    StructField("resultFiles", ArrayType(StructType([
        StructField("location",  StringType(), True),
        StructField("sizeBytes", LongType(),   True),
    ])), True),
])


# =============================================================================
# HELPERS — parse LZ string columns into typed structs before transforming
# =============================================================================

def parse_lzn_orders(lz_df):
    return (
        lz_df
        .withColumn("_orders_parsed", from_json(col("orders"), ArrayType(ORDER_SCHEMA)))
        .withColumn("o", explode(col("_orders_parsed")))
    )

def parse_lzn_results(lz_df):
    return (
        lz_df
        .withColumn("_metadata_parsed",  from_json(col("metadata"),          METADATA_SCHEMA))
        .withColumn("_results_parsed",   from_json(col("results"),           ArrayType(RESULT_ITEM_SCHEMA)))
        .withColumn("_order_txn_parsed", from_json(col("order_transaction"), ORDER_TXN_SCHEMA))
    )

def parse_lzn_manifest(lz_df):
    return (
        lz_df
        .withColumn("_job_parsed",     from_json(col("job"),     MANIFEST_JOB_SCHEMA))
        .withColumn("_summary_parsed", from_json(col("summary"), MANIFEST_SUMMARY_SCHEMA))
        .withColumn("_files_parsed",   from_json(col("files"),   MANIFEST_FILES_SCHEMA))
    )


# =============================================================================
# TABLE 1: raw_manifest
# =============================================================================
@dlt.table(
    name    = "manifests",
    comment = "RAW: Flattened manifest — one row per API batch run.",
    table_properties = {"quality": "raw", "delta.enableChangeDataFeed": "true"}
)
def raw_manifest():
    lz = dlt.read_stream(LZN_MANIFEST)
    parsed = parse_lzn_manifest(lz)
    return parsed.select(
        col("_job_parsed.type")                   .alias("job_type"),
        col("_job_parsed.folderName")             .alias("batch_folder"),
        to_timestamp(col("_job_parsed.startTime")).alias("job_start_time"),
        to_timestamp(col("_job_parsed.endTime"))  .alias("job_end_time"),
        col("_job_parsed.durationMs")             .alias("job_duration_ms"),
        col("_summary_parsed.ordersProcessed")    .alias("orders_processed"),
        col("_summary_parsed.resultsProcessed")   .alias("results_processed"),
        col("_summary_parsed.errorsCount")        .alias("errors_count"),
        col("_summary_parsed.totalFileSize")      .alias("total_file_size_bytes"),
        col("_summary_parsed.totalFileSizeHuman") .alias("total_file_size_human"),
        col("_files_parsed.ordersFile.location")  .alias("orders_file_s3_location"),
        col("_files_parsed.ordersFile.sizeBytes") .alias("orders_file_size_bytes"),
        col("_batch_folder"),
        col("_source_file"),
        col("_ingested_at"),
    )


# =============================================================================
# TABLE 2: raw_orders
# =============================================================================
@dlt.table(
    name    = "orders",
    comment = "RAW: One row per lab order. Exploded from all_orders.json orders[].",
    table_properties = {"quality": "raw", "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_user_id",  "user_id IS NOT NULL")
def raw_orders():
    lz = dlt.read_stream(LZN_ORDERS)
    parsed = parse_lzn_orders(lz)
    return parsed.select(
        col("o.id")                                     .alias("order_id"),
        col("o.user_id")                                .alias("user_id"),
        col("o.team_id")                                .alias("team_id"),
        col("o.status")                                 .alias("order_status"),
        col("o.origin")                                 .alias("order_origin"),
        col("o.priority")                               .alias("priority"),
        col("o.billing_type")                           .alias("billing_type"),
        col("o.has_abn")                                .alias("has_abn"),
        col("o.interpretation")                         .alias("order_interpretation"),
        col("o.has_missing_results")                    .alias("has_missing_results"),
        col("o.icd_codes")                              .alias("icd_codes"),
        col("o.sample_id")                              .alias("sample_id"),
        col("o.notes")                                  .alias("order_notes"),
        col("o.health_insurance_id")                    .alias("health_insurance_id"),
        col("o.passthrough")                            .alias("passthrough"),
        col("o.requisition_form_url")                   .alias("requisition_form_url"),
        to_timestamp(col("o.created_at"))               .alias("order_created_at"),
        to_timestamp(col("o.updated_at"))               .alias("order_updated_at"),
        to_timestamp(col("o.activate_by"))              .alias("activate_by"),
        to_timestamp(col("o.expected_result_by_date"))  .alias("expected_result_by_date"),
        to_timestamp(col("o.worst_case_result_by_date")).alias("worst_case_result_by_date"),
        col("o.patient_details.first_name")             .alias("patient_first_name"),
        col("o.patient_details.last_name")              .alias("patient_last_name"),
        to_timestamp(col("o.patient_details.dob"))      .alias("patient_dob"),
        col("o.patient_details.gender")                 .alias("patient_gender"),
        col("o.patient_details.phone_number")           .alias("patient_phone"),
        col("o.patient_details.email")                  .alias("patient_email"),
        col("o.patient_details.medical_proxy")          .alias("patient_medical_proxy"),
        col("o.patient_details.race")                   .alias("patient_race"),
        col("o.patient_details.ethnicity")              .alias("patient_ethnicity"),
        col("o.patient_details.sexual_orientation")     .alias("patient_sexual_orientation"),
        col("o.patient_details.gender_identity")        .alias("patient_gender_identity"),
        col("o.patient_address.receiver_name")          .alias("addr_receiver_name"),
        col("o.patient_address.first_line")             .alias("addr_line1"),
        col("o.patient_address.second_line")            .alias("addr_line2"),
        col("o.patient_address.city")                   .alias("addr_city"),
        col("o.patient_address.state")                  .alias("addr_state"),
        col("o.patient_address.zip")                    .alias("addr_zip"),
        col("o.patient_address.country")                .alias("addr_country"),
        col("o.patient_address.phone_number")           .alias("addr_phone"),
        col("o.lab_test.id")                            .alias("lab_test_id"),
        col("o.lab_test.slug")                          .alias("lab_test_slug"),
        col("o.lab_test.name")                          .alias("lab_test_name"),
        col("o.lab_test.sample_type")                   .alias("lab_sample_type"),
        col("o.lab_test.method")                        .alias("lab_collection_method"),
        col("o.lab_test.price")                         .alias("lab_test_price"),
        col("o.lab_test.is_active")                     .alias("lab_test_is_active"),
        col("o.lab_test.status")                        .alias("lab_test_status"),
        col("o.lab_test.fasting")                       .alias("fasting_required"),
        col("o.lab_test.is_delegated")                  .alias("lab_test_is_delegated"),
        col("o.lab_test.auto_generated")                .alias("lab_test_auto_generated"),
        col("o.lab_test.common_tat_days")               .alias("common_tat_days"),
        col("o.lab_test.worst_case_tat_days")           .alias("worst_case_tat_days"),
        col("o.lab_test.lab.id")                        .alias("lab_id"),
        col("o.lab_test.lab.slug")                      .alias("lab_slug"),
        col("o.lab_test.lab.name")                      .alias("lab_name"),
        col("o.lab_test.lab.first_line_address")        .alias("lab_address"),
        col("o.lab_test.lab.city")                      .alias("lab_city"),
        col("o.lab_test.lab.zipcode")                   .alias("lab_zipcode"),
        col("o.physician.first_name")                   .alias("physician_first_name"),
        col("o.physician.last_name")                    .alias("physician_last_name"),
        col("o.physician.npi")                          .alias("physician_npi"),
        col("o.details.type")                           .alias("details_type"),
        col("o.details.data.id")                        .alias("details_data_id"),
        to_timestamp(col("o.details.data.created_at"))  .alias("details_data_created_at"),
        col("o.details.data.appointment_id")            .alias("details_appointment_id"),
        col("o.order_transaction.id")                   .alias("order_transaction_id"),
        col("o.order_transaction.status")               .alias("order_transaction_status"),
        col("_batch_folder"),
        col("_source_file"),
        col("_ingested_at"),
    )


# =============================================================================
# TABLE 3: raw_order_markers
# =============================================================================
@dlt.table(
    name    = "order_markers",
    comment = "RAW: One row per ordered marker/panel per order.",
    table_properties = {"quality": "raw", "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_drop("valid_order_id",  "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_marker_id", "marker_id IS NOT NULL")
def raw_order_markers():
    lz = dlt.read_stream(LZN_ORDERS)
    parsed = parse_lzn_orders(lz)
    return (
        parsed
        .withColumn("m", explode(col("o.lab_test.markers")))
        .select(
            col("o.id")                  .alias("order_id"),
            col("o.user_id")             .alias("user_id"),
            col("o.lab_test.id")         .alias("lab_test_id"),
            col("m.id")                  .alias("marker_id"),
            col("m.name")                .alias("marker_name"),
            col("m.slug")                .alias("marker_slug"),
            col("m.description")         .alias("marker_description"),
            col("m.lab_id")              .alias("marker_lab_id"),
            col("m.provider_id")         .alias("marker_provider_id"),
            col("m.type")                .alias("marker_type"),
            col("m.unit")                .alias("marker_unit"),
            col("m.price")               .alias("marker_price"),
            col("m.aoe")                 .alias("marker_aoe"),
            col("m.a_la_carte_enabled")  .alias("a_la_carte_enabled"),
            col("m.common_tat_days")     .alias("common_tat_days"),
            col("m.worst_case_tat_days") .alias("worst_case_tat_days"),
            col("m.is_orderable")        .alias("is_orderable"),
            col("_batch_folder"),
            col("_source_file"),
            col("_ingested_at"),
        )
    )


# =============================================================================
# TABLE 4: raw_order_events
# =============================================================================
@dlt.table(
    name    = "order_events",
    comment = "RAW: One row per status event per order.",
    table_properties = {"quality": "raw", "delta.enableChangeDataFeed": "true"}
)
def raw_order_events():
    lz = dlt.read_stream(LZN_ORDERS)
    parsed = parse_lzn_orders(lz)
    return (
        parsed
        .withColumn("ev", explode_outer(col("o.events")))
        .select(
            col("o.id")                      .alias("order_id"),
            col("o.user_id")                 .alias("user_id"),
            col("ev.id")                     .alias("event_id"),
            col("ev.status")                 .alias("event_status"),
            to_timestamp(col("ev.created_at")).alias("event_created_at"),
            col("_batch_folder"),
            col("_source_file"),
            col("_ingested_at"),
        )
    )


# =============================================================================
# TABLE 5: raw_results
# =============================================================================
@dlt.table(
    name           = "results",
    comment        = "RAW: One row per biomarker result. Partitioned by report_date.",
    partition_cols = ["report_date"],
    table_properties = {
        "quality": "raw",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_drop("valid_patient_id",      "patient_id IS NOT NULL")
@dlt.expect_or_drop("valid_specimen_number", "specimen_number IS NOT NULL")
@dlt.expect_or_drop("valid_loinc",           "loinc IS NOT NULL")
def raw_results():
    lz = dlt.read_stream(LZN_RESULTS)
    parsed = parse_lzn_results(lz)
    return (
        parsed
        .withColumn("r", explode(col("_results_parsed")))
        .select(
            col("_metadata_parsed.patient_id")       .alias("patient_id"),
            col("_metadata_parsed.account_id")       .alias("account_id"),
            col("_metadata_parsed.patient")          .alias("patient_name"),
            col("_metadata_parsed.dob")              .alias("patient_dob"),
            col("_metadata_parsed.age")              .alias("patient_age"),
            col("_metadata_parsed.provider")         .alias("provider_name"),
            col("_metadata_parsed.laboratory")       .alias("laboratory"),
            col("_metadata_parsed.specimen_number")  .alias("specimen_number"),
            col("_metadata_parsed.clia_#")           .alias("clia_number"),
            col("_metadata_parsed.status")           .alias("report_status"),
            col("_metadata_parsed.interpretation")   .alias("report_interpretation"),
            to_timestamp(col("_metadata_parsed.date_collected")).alias("date_collected"),
            to_timestamp(col("_metadata_parsed.date_received")) .alias("date_received"),
            to_timestamp(col("_metadata_parsed.date_reported")) .alias("date_reported"),
            col("r.name")                            .alias("biomarker_name"),
            col("r.slug")                            .alias("biomarker_slug"),
            col("r.loinc")                           .alias("loinc"),
            col("r.loinc_slug")                      .alias("loinc_slug"),
            col("r.provider_id")                     .alias("provider_id"),
            col("r.value")                           .alias("value"),
            col("r.result")                          .alias("raw_result"),
            col("r.type")                            .alias("result_type"),
            col("r.unit")                            .alias("unit"),
            col("r.reference_range")                 .alias("reference_range"),
            col("r.min_range_value")                 .alias("min_range_value"),
            col("r.max_range_value")                 .alias("max_range_value"),
            col("r.is_above_max_range")              .alias("is_high"),
            col("r.is_below_min_range")              .alias("is_low"),
            col("r.interpretation")                  .alias("result_interpretation"),
            col("r.notes")                           .alias("result_notes"),
            col("r.performing_laboratory")           .alias("performing_laboratory"),
            col("r.source_sample_id")                .alias("source_sample_id"),
            to_timestamp(col("r.timestamp"))         .alias("result_timestamp"),
            col("_order_txn_parsed.id")              .alias("order_transaction_id"),
            col("_order_txn_parsed.status")          .alias("order_transaction_status"),
            to_timestamp(col("_metadata_parsed.date_reported")).cast("date").alias("report_date"),
            col("_batch_folder"),
            col("_source_file"),
            col("_ingested_at"),
        )
    )


# =============================================================================
# TABLE 6: raw_result_source_markers
# =============================================================================
@dlt.table(
    name    = "result_source_markers",
    comment = "RAW: One row per source_marker per result. Links results to ordering panel.",
    table_properties = {"quality": "raw", "delta.enableChangeDataFeed": "true"}
)
def raw_result_source_markers():
    lz = dlt.read_stream(LZN_RESULTS)
    parsed = parse_lzn_results(lz)
    return (
        parsed
        .withColumn("r",  explode(col("_results_parsed")))
        .withColumn("sm", explode_outer(col("r.source_markers")))
        .select(
            col("_metadata_parsed.patient_id")       .alias("patient_id"),
            col("_metadata_parsed.specimen_number")  .alias("specimen_number"),
            col("_order_txn_parsed.id")              .alias("order_transaction_id"),
            col("r.loinc")                           .alias("loinc"),
            col("r.loinc_slug")                      .alias("loinc_slug"),
            col("r.provider_id")                     .alias("result_provider_id"),
            col("r.slug")                            .alias("biomarker_slug"),
            col("sm.marker_id")                      .alias("marker_id"),
            col("sm.name")                           .alias("marker_name"),
            col("sm.slug")                           .alias("marker_slug"),
            col("sm.provider_id")                    .alias("marker_provider_id"),
            col("_batch_folder"),
            col("_source_file"),
            col("_ingested_at"),
        )
    )


# =============================================================================
# TABLE 7: raw_order_transaction_orders
# =============================================================================
@dlt.table(
    name    = "order_transaction_orders",
    comment = "RAW: Child orders inside order_transaction.orders[]. From result files.",
    table_properties = {"quality": "raw", "delta.enableChangeDataFeed": "true"}
)
def raw_order_transaction_orders():
    lz = dlt.read_stream(LZN_RESULTS)
    parsed = parse_lzn_results(lz)
    return (
        parsed
        .withColumn("txn_order", explode(col("_order_txn_parsed.orders")))
        .select(
            col("_order_txn_parsed.id")                           .alias("order_transaction_id"),
            col("_order_txn_parsed.status")                       .alias("order_transaction_status"),
            col("txn_order.id")                                   .alias("order_id"),
            col("txn_order.low_level_status")                     .alias("low_level_status"),
            to_timestamp(col("txn_order.low_level_status_created_at")).alias("low_level_status_created_at"),
            col("txn_order.origin")                               .alias("order_origin"),
            col("txn_order.parent_id")                            .alias("parent_order_id"),
            to_timestamp(col("txn_order.created_at"))             .alias("order_created_at"),
            to_timestamp(col("txn_order.updated_at"))             .alias("order_updated_at"),
            col("_metadata_parsed.patient_id")                    .alias("patient_id"),
            col("_metadata_parsed.specimen_number")               .alias("specimen_number"),
            col("_batch_folder"),
            col("_source_file"),
            col("_ingested_at"),
        )
    )