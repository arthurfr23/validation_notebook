# Fabric notebook source

# METADATA ********************


# CELL ********************

import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from typing import List

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validation_dataframes(df_from: DataFrame, df_to: DataFrame, keys: List[str], except_columns: List[str] = ['']):

    """
    Validates and compares two PySpark DataFrames, identifying differences in records and columns.

    This function performs several checks:
    - Verifies that all provided keys exist in both input DataFrames.
    - Counts the total and distinct records in both DataFrames based on the provided keys.
    - Identifies coincident columns between the two DataFrames, excluding specified columns.
    - Performs an inner join on the deduplicated DataFrames using a null-safe equality condition on the keys.
    - Compares the values of coincident columns for the joined records.
    - Prints summary statistics about the columns and records.
    - Displays the records where the values in the coincident columns differ.

    Args:
        df_from (DataFrame): The source PySpark DataFrame.
        df_to (DataFrame): The target PySpark DataFrame to compare against.
        keys (List[str]): A list of column names to use as keys for deduplication and joining.
        except_columns (List[str], optional): A list of column names to exclude from the value comparison.
            Defaults to [''], effectively excluding no columns by default.

    Returns:
        DataFrame: A DataFrame containing the records that are present in both `df_from` and `df_to`
                   after deduplication based on the `keys`.

    Raises:
        ValueError: If any of the provided `keys` are not present in both input DataFrames.

    Examples:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("validation_example").getOrCreate()
        >>> data_from = [(1, 'A', 10), (1, 'A', 10), (2, 'B', 20), (None, 'C', 30)]
        >>> df_from = spark.createDataFrame(data_from, ["id", "letter", "value"])
        >>> data_to = [(1, 'A', 10), (2, 'B', 25), (None, 'C', 30), (4, 'D', 40)]
        >>> df_to = spark.createDataFrame(data_to, ["id", "letter", "value"])
        >>> join_df = validation_dataframes(df_from, df_to, ["id", "letter"])
        ------ Columns
        Count columns "from": 3
        Count columns "to": 3
        Coincident columns: 3
        Count column only in "from": 0
        Count column only in "to": 0

        ------ Records
        Coincident and non-duplicated to compare: 3
        Records "from": 4 / 1 duplicated / 0 not present in "to" records
        Records "to": 4 / 0 duplicated / 1 not present in "from" records

        ------ Diferences
        +-------+-------+-----------+-----------------+-----------------+-----------+
        |id     |letter |nome_coluna|column_value_from|column_value_to|check_equal|
        +-------+-------+-----------+-----------------+-----------------+-----------+
        |2      |B      |value      |20               |25               |false      |
        +-------+-------+-----------+-----------------+-----------------+-----------+
        <BLANKLINE>
        >>> join_df.show()
        +----+------+-----+----+------+
        |  id|letter|value|  id|letter|
        +----+------+-----+----+------+
        |   1|     A|   10|   1|     A|
        |   2|     B|   20|   2|     B|
        |null|     C|   30|null|     C|
        +----+------+-----+----+------+
        <BLANKLINE>
        >>> spark.stop()
    """
   
    # Getting the coincident columns from "from" and "to"
    columns_from = set(df_from.columns)
    columns_to = set(df_to.columns)
    coincident_columns = columns_from.intersection(columns_to)

    # check if the keys is on the coincident columns
    check_keys = set(keys).intersection(coincident_columns)
    if len(check_keys) != len(keys):
        raise ValueError("The keys provided in the parameter are not fully present in either dataframe.")

    # Counting the records from the "from" dataframe
    count_from_records = df_from.count()

    # Deduplicating the "from" dataframe based on the keys columns
    df_from_dedup = df_from.drop_duplicates(keys)

    # Counting deduplicated records from "from" dataframe
    count_distinct_from_records = df_from_dedup.count()
                    
    # Counting the records from "to" dataframe
    count_to_records = df_to.count()

    # Deduplicating the "to" dataframe based on the keys columns
    df_to_dedup = df_to.dropDuplicates(keys)

    # Counting deduplicated records from "to" dataframe 
    count_distinct_to_records = df_to_dedup.count()

    # Removing the except_columns to not consider in the comparing  
    columns = [col for col in coincident_columns if col not in except_columns]

    # Adjust the key to prevent from null columns 
    join_keys_list = [f"(from.{key} <=> to.{key})" for key in keys]
    join_keys_string = " and ".join(join_keys_list)

    # Joining the "from" and "to" deduplicated dataframes
    df_join = (df_from_dedup.alias('from').join(df_to_dedup.alias('to'), f.expr(join_keys_string), 'inner'))

    # Counting the records matched after the inner join
    count_df_join = df_join.count()

    # Comparison process to identify the differences between the "from" and "to" deduplicated dataframes
    df_result = (df_join.select(f.array(*[f.struct(
                *[f.col('from.' + key).alias(key + "") for key in keys],
                f.lit(col).cast(StringType()).alias('nome_coluna'),
                f.col('from.' + col).cast(StringType()).alias('column_value_from'),
                f.col('to.' + col).cast(StringType()).alias('column_value_to'),
                f.col('from.' + col).eqNullSafe(f.col('to.' + col)).alias('check_equal')
                ) for col in columns]).alias('cols'))
                .select(f.inline('cols'))
                .where('check_equal = false'))

    # Results

    # COLUMN
    print('------ Columns')
    print('Count columns "from":', len(columns_from))
    print('Count columns "to":', len(columns_to))
    print('Coincident columns:', len(columns_from.intersection(columns_to)))
    print('Count column only in "from":', len(columns_from - columns_to),", ".join(columns_from - columns_to))
    print('Count column only in "to":', len(columns_to - columns_from),", ".join(columns_to - columns_from))
    print()

    # RECORDS
    print('------ Records')
    print('Coincident and non-duplicated to compare:', count_df_join)
    print('Records "from":', f"{count_from_records}", f" / {count_from_records - count_distinct_from_records} duplicated", f' / {count_distinct_from_records - count_df_join} not present in "to" records')
    print('Records "to":', f"{count_to_records}", f" / {count_to_records - count_distinct_to_records} duplicated", f' / {count_distinct_to_records - count_df_join} not present in "from" records')
    print()

    # DIFERENCES (Display)
    print('------ Diferences')
    if df_result.count() > 0:
        display(df_result)
    else:
        print('No diferences found.')

    # Joining full the "from" and "to" deduplicated dataframes to return
    df_join = (df_from_dedup.alias('from').join(df_to_dedup.alias('to'), f.expr(join_keys_string), 'full'))

    return df_join

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
