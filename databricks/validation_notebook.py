import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from typing import List, Dict

def compare_dataframes_with_summary(
    df_from: DataFrame,
    df_to: DataFrame,
    keys: List[str],
    except_columns: List[str] = None
) -> Dict[str, DataFrame]:

    except_columns = except_columns or []

    columns_from = set(df_from.columns)
    columns_to = set(df_to.columns)
    coincident_columns = sorted(list(columns_from.intersection(columns_to)))


    missing = [k for k in keys if k not in coincident_columns]
    if missing:
        raise ValueError(f"Keys não existem nos dois DFs: {missing}")

    compare_cols = [c for c in coincident_columns if c not in except_columns]

    df_from_dedup = df_from.dropDuplicates(keys)
    df_to_dedup   = df_to.dropDuplicates(keys)


    join_expr = " AND ".join([f"(from.`{k}` <=> to.`{k}`)" for k in keys])

    full = (
        df_from_dedup.alias("from")
        .join(df_to_dedup.alias("to"), f.expr(join_expr), "full")
    )

    only_from = (
        full
        .where(f.expr(" AND ".join([f"to.`{k}` IS NULL AND from.`{k}` IS NOT NULL" for k in keys])))
        .select([f.col(f"from.`{k}`").alias(k) for k in keys])
        .distinct()
    )

    only_to = (
        full
        .where(f.expr(" AND ".join([f"from.`{k}` IS NULL AND to.`{k}` IS NOT NULL" for k in keys])))
        .select([f.col(f"to.`{k}`").alias(k) for k in keys])
        .distinct()
    )

    inner = (
        df_from_dedup.alias("from")
        .join(df_to_dedup.alias("to"), f.expr(join_expr), "inner")
    )

    diff_values = (
        inner
        .select(
            f.array(*[
                f.struct(
                    *[f.col(f"from.`{k}`").alias(k) for k in keys],
                    f.lit(c).cast(StringType()).alias("coluna"),
                    f.col(f"from.`{c}`").cast(StringType()).alias("valor_from"),
                    f.col(f"to.`{c}`").cast(StringType()).alias("valor_to"),
                    f.col(f"from.`{c}`").eqNullSafe(f.col(f"to.`{c}`")).alias("igual")
                )
                for c in compare_cols
            ]).alias("cols")
        )
        .select(f.inline("cols"))
        .where("igual = false")
        .drop("igual")
    )

    from_total = df_from.count()
    to_total = df_to.count()
    from_dedup = df_from_dedup.count()
    to_dedup = df_to_dedup.count()
    matched = inner.count()

    only_from_cnt = only_from.count()
    only_to_cnt = only_to.count()
    diff_cnt = diff_values.count()

    summary = df_from.sparkSession.createDataFrame([{
        "from_total_rows": from_total,
        "from_dedup_rows": from_dedup,
        "from_duplicates_removed": from_total - from_dedup,
        "to_total_rows": to_total,
        "to_dedup_rows": to_dedup,
        "to_duplicates_removed": to_total - to_dedup,
        "matched_keys": matched,
        "only_in_from_keys": only_from_cnt,
        "only_in_to_keys": only_to_cnt,
        "diff_cells": diff_cnt
    }])

    column_summary = df_from.sparkSession.createDataFrame([{
        "from_columns": len(columns_from),
        "to_columns": len(columns_to),
        "coincident_columns": len(coincident_columns),
        "only_in_from_columns": len(columns_from - columns_to),
        "only_in_to_columns": len(columns_to - columns_from),
        "ignored_columns": len(except_columns),
        "compared_columns": len(compare_cols)
    }])

    return {
        "summary": summary,
        "column_summary": column_summary,
        "diff_values": diff_values,
        "only_from": only_from,
        "only_to": only_to,
        "full_join": full
    }
