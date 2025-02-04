from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.utils import AnalysisException
import logging
from datetime import datetime

# Tiyambe matsenga a Pyspark zimenezi!!
source_db_config = {
    "url": "jdbc:mysql://localhost:3306/<schema1>",
    "user": "",
    "password": ""
}

target_db_config = {
    "url": "jdbc:mysql://localhost:3306/<schema2>",
    "user": "",
    "password": ""
}

table_list = [
    "<table_name>"
]


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Matsenga - Extract Transform Load: From mysql server 1 to mysql server 2") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.extraClassPath", "<path to:/mysql-connector-java-8.0.29.jar>").getOrCreate()

# Matsenga awawa ongojudula data chabe


def get_latest_pull_number(table_name):
    try:
        # Load only the pull_number column from the target table
        df_target_pull = spark.read \
            .format("jdbc") \
            .option("url", target_db_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", target_db_config["user"]) \
            .option("password", target_db_config["password"]) \
            .load() \
            .select(F.max("pull_number").alias("max_pull_number"))

        # Get the maximum pull number
        latest_pull_number = df_target_pull.collect()[0]["max_pull_number"]
        return latest_pull_number + 1 if latest_pull_number else 1
    except AnalysisException:
        # Return 1 if table or column does not exist
        return 1


def clean_data(df):
    df = df.dropna(subset=["patient_id"])

    df = df.dropDuplicates(["patient_id", "site_id"])

    df = df.fillna({"gender": "Unknown"})

    df = df.withColumn("patient_id", col("patient_id").cast("string"))

    return df

# Below function is for loading the data incrementally


def incremental_load(df, table_name):
    current_pull_number = get_latest_pull_number(table_name)
    df = df.withColumn("pull_number", F.lit(current_pull_number))
    return df


# Now let's loop through each table and perform the ETL process - Tijuje zimenezi!
for table_name in table_list:
    print(f"Processing table: {table_name}")

    # Eheem let's extract data from source MySQL table (including incremental logic)
    try:
        source_df = spark.read \
            .format("jdbc") \
            .option("url", source_db_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", source_db_config["user"]) \
            .option("password", source_db_config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        logging.info(f"Extracted data from {table_name}.")
    except Exception as e:
        logging.error(f"Error extracting data from {table_name}: {str(e)}")
        continue

    cleaned_df = clean_data(source_df)

    incremental_df = incremental_load(cleaned_df, table_name)

    # Below function will do matsenga oika data into the target MySQL table
    try:
        incremental_df.write \
            .format("jdbc") \
            .option("url", target_db_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", target_db_config["user"]) \
            .option("password", target_db_config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append").save()
        logging.info(
            f"Data successfully loaded into {table_name} with pull_number {get_latest_pull_number(table_name)-1}.")
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {str(e)}")

# Stop Spark session - Osamaiwala kutere ndithu otherwise muthyola!!
spark.stop()
