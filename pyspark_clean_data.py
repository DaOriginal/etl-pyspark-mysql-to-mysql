# %%
import pandas as pd
from pyspark.sql import (
    SparkSession,
    types,
    functions as F,
)

spark = (
    SparkSession
    .builder
    .appName('tiyambe_pyspark')
    .getOrCreate()
)

# %%
report_1_data = spark.read.csv(
    'processed_files/REPORT_1_amalgamated.csv', header=True)
report_2_data = spark.read.csv(
    'processed_files/REPORT_2_amalgamated.csv', header=True)
report_3_data = spark.read.csv(
    'processed_files/REPORT_3_amalgamated.csv', header=True)
report_4_data = spark.read.csv(
    'processed_files/REPORT_4_amalgamated.csv', header=True)

# %%
clean_report_1 = report_1_data.filter(~F.col("facility").contains("facility"))
clean_report_2 = report_2_data.filter(~F.col("facility").contains("facility"))
clean_report_3 = report_3_data.filter(~F.col("facility").contains("facility"))
clean_report_4 = report_4_data.filter(~F.col("facility").contains("facility"))

# %%
clean_report_1 = clean_report_1.toPandas()
clean_report_2 = clean_report_2.toPandas()
clean_report_3 = clean_report_3.toPandas()
clean_report_4 = clean_report_4.toPandas()

# %%
print(type(clean_report_1))

# %%
with pd.ExcelWriter("all_reports.xlsx") as writer:
    clean_report_1.to_excel(writer, sheet_name="REPORT_1", index=False)
    clean_report_2.to_excel(writer, sheet_name="REPORT_2", index=False)
    clean_report_3.to_excel(writer, sheet_name="REPORT_3", index=False)
    clean_report_4.to_excel(writer, sheet_name="REPORT_4", index=False)

spark.stop()
