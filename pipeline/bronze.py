"""
Bronze Layer: Lakeflow declarative pipeline.

Lands raw S3 data parquet files as delta streaming tables. No transformations are done
since this is bronze layer.

We do add two metadata columns to the tables:
 1. ingested_at
 2. source file

Meant to help add auditability on what was ingested and when.

No schemas are declared for this layer, parquet carries column names and types
in its footer, so autoloader infers them and tracks which files it has already consumed.
Meaning, pipeline will only read new files that didn't exist in prior runs

pipeline configuration expected:
    raw_path s3://<datalake-bucket>/raw

"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import DataFrame

RAW = spark.conf.get("raw_path")


def bronze_table(table_name: str, prefix: str, comment: str = "") -> None:
    """
    Define one bronze streaming table over a raw prefix

    A factory rather than one function per table, each call getting its own closure,
    so the function binds the right arguments.

    Defining these inline in a loop would not, because the loop variable would be looked up at
    each call time rather than captured
    """

    @dp.table(name=table_name, comment=comment)
    def _bronze() -> DataFrame:
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            # addNewColumns: a column the schema has not seen is added to the
            # table rather than being buried in _rescued_data. The update that
            # first encounters it fails, the schema is updated, and the retry
            # succeeds — so drift costs one failed run and then self-heals.
            #
            # Rows written before a column existed keep null for it, which is
            # accurate: the source did not have it then. Backfilling history
            # would need a full refresh.
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{RAW}/{prefix}/")
            .select(
                "*",
                col("_metadata.file_path").alias("source_file"),
                current_timestamp().alias("ingested_at")
            )
        )


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

bronze_table(
    "bronze.pbp",
    "pbp",
    comment="Play-by-play, 1999 to current. 372 columns, stable across every season.",
)


# ---------------------------------------------------------------------------
# Remaining sources — uncomment as each is verified
# ---------------------------------------------------------------------------
#
# The stat-type variants are separate tables rather than one per source. They
# are not the same shape: nextgen_stats is 29/23/22 columns across
# passing/receiving/rushing with only 11 shared, and pfr_advstats is
# 24/16/17/29 across pass/rush/rec/def with 9 shared. Unioning them would give
# a wide, mostly-null table whose columns mean different things per row.
#
# bronze_table(
#     "bronze.players", "players",
#     comment="Full player table, snapshotted per ingest_date. Not season-scoped.",
# )
#
# bronze_table(
#     "bronze.teams", "teams",
#     comment="Team reference data, snapshotted per ingest_date.",
# )
#
# bronze_table(
#     "bronze.participation", "participation",
#     comment="On-field personnel per play, 2016 to current season minus one.",
# )
#
# for _stat in ["passing", "receiving", "rushing"]:
#     bronze_table(
#         f"bronze.nextgen_{_stat}",
#         f"nextgen_stats/stat_type={_stat}",
#         comment=f"Next Gen Stats, {_stat}, weekly player level. 2016 to current.",
#     )
#
# for _stat in ["pass", "rush", "rec", "def"]:
#     bronze_table(
#         f"bronze.pfr_{_stat}",
#         f"pfr_advstats/stat_type={_stat}",
#         comment=f"PFR advanced stats, {_stat}, weekly. 2018 to current.",
#     )
