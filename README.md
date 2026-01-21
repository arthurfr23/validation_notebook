# validation_notebook

These notebooks are useful for validating results between source and target tables.

These .py files need to be called from another notebook in the following way:

1) Databricks

df_from = spark.sql("""
-- source query here
""")

df_to = spark.sql("""
-- target query here
""")       

keys = []
except_columns = []

out = compare_dataframes_with_summary(df_from, df_to, keys, except_columns)

display(out["column_summary"])
display(out["summary"])

display(out["diff_values"])
display(out["only_from"])
display(out["only_to"])

2) Fabric

df_from = spark.sql("""
-- source query here
""")

df_to = spark.sql("""
-- target query here
""")       


keys = ['YYYYQ', 'Iname']

df = validation_dataframes(df_from, df_to, keys)