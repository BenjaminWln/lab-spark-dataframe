#!/usr/bin/env python3
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

USE_S3 = "AWS_S3_ENDPOINT" in os.environ and "AWS_ACCESS_KEY_ID" in os.environ
SKIP_ICEBERG = os.environ.get("SKIP_ICEBERG") == "1"

if USE_S3:
    os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
    BUCKET = os.environ["LAB_BUCKET_NAME"]
    BASE = f"s3a://{BUCKET}"
    DUCK_BASE = f"s3://{BUCKET}"
else:
    LOCAL_ROOT = os.path.abspath("data")
    BASE = Path(LOCAL_ROOT).as_uri()
    DUCK_BASE = LOCAL_ROOT
    BUCKET = "local"
print("Storage root:", BASE)

def list_objects(prefix):
    if USE_S3:
        subprocess.run(["aws", "s3", "--profile", "default", "ls", f"{DUCK_BASE}/{prefix}", "--recursive"], check=True)
    else:
        root = os.path.join(DUCK_BASE, prefix)
        for dirpath, _, files in sorted(os.walk(root)):
            for name in sorted(files):
                p = os.path.join(dirpath, name)
                print(f"{os.path.getsize(p):>10}  {os.path.relpath(p, DUCK_BASE)}")

def duck(sql):
    import duckdb

    con = duckdb.connect()
    if USE_S3:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
    con.sql(sql).show(max_width=200)

if not USE_S3 and not (
    os.path.exists("data/bronze/users.csv") and os.path.exists("data/bronze/orders.csv")
):
    subprocess.run([sys.executable, "scripts/generate_datasets.py", "--out", "data/bronze"], check=True)
list_objects("bronze/")

import pyspark

print("PySpark version:", pyspark.__version__)
if USE_S3:
    os.system("ls $SPARK_HOME/jars | grep -i hadoop-aws")

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = os.environ.get("ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0")

spark_builder = SparkSession.builder.appName("lab-silver-pyspark").master("local[*]")

if USE_S3:
    spark_builder = (
        spark_builder
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

if not SKIP_ICEBERG:
    spark_builder = (
        spark_builder
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"{BASE}/warehouse")
    )

spark = spark_builder.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(spark)

users_bronze = spark.read.csv(
    f"{BASE}/bronze/users.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
orders_bronze = spark.read.csv(
    f"{BASE}/bronze/orders.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)

users_bronze.printSchema()
users_bronze.show(3, truncate=False)

users_bronze.select("address").show(3, truncate=False)

from pyspark.sql import functions as F

users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)

stg_users = stg_users.withColumn(
    "city", F.when(F.col("city") == "", None).otherwise(F.col("city"))
)

stg_users.show(5, truncate=False)

from pyspark.sql.window import Window

orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)

print("Original:", orders_typed.count())

duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)

print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates \
    .groupBy("order_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

dedup_window = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("ordered_at").desc())
)

stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

print("After deduplication:", stg_orders.count())

stg_orders \
    .groupBy("order_id") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

orders_typed.createOrReplaceTempView("orders_typed")

stg_orders_sql = spark.sql('''
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
''')

stg_orders_sql.show(5)

assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)

stg_users.createOrReplaceTempView("stg_users")

spark.sql('''
    SELECT state,
           count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users
    GROUP BY state
    ORDER BY users DESC
''').show(10)

stg_users.write.mode("overwrite").parquet(f"{BASE}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"{BASE}/silver/spark/orders.parquet")

list_objects("silver/")

users_silver = spark.read.parquet(f"{BASE}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"{BASE}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()

if not SKIP_ICEBERG:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")

    stg_users.writeTo("lab.silver.users").createOrReplace()
    stg_orders.writeTo("lab.silver.orders").createOrReplace()

    list_objects("warehouse/")

    spark.sql("""
        SELECT snapshot_id, operation, committed_at
        FROM lab.silver.orders.snapshots
    """).show(truncate=False)

    spark.sql("""
        SELECT file_path, file_format, record_count, file_size_in_bytes
        FROM lab.silver.orders.files
    """).show(truncate=False)

    users_iceberg = spark.table("lab.silver.users")
    orders_iceberg = spark.table("lab.silver.orders")
    assert users_iceberg.count() == users_bronze.count()
    assert orders_iceberg.count() == stg_orders.count()

def sync_warehouse():
    if USE_S3:
        subprocess.run(
            ["aws", "s3", "--profile", "default", "sync", f"{DUCK_BASE}/warehouse/", "./warehouse/"],
            check=True,
        )
    elif os.path.exists(os.path.join(DUCK_BASE, "warehouse")):
        shutil.copytree(os.path.join(DUCK_BASE, "warehouse"), "./warehouse", dirs_exist_ok=True)

if not SKIP_ICEBERG:
    sync_warehouse()
    print(
        "Now commit only the metadata (data files are git-ignored):\n"
        "  git add ./warehouse/silver/orders/metadata ./warehouse/silver/users/metadata\n"
        '  git commit -m "feat(spark-lab): add Iceberg table metadata"\n'
        "  git push"
    )

DBT_SILVER = os.environ.get("DBT_SILVER_DIR", f"{DUCK_BASE}/silver/dbt")
dbt_available = USE_S3 or os.path.exists(f"{DBT_SILVER}/orders.parquet")

if dbt_available:
    duck(f"""
      SELECT 'dbt' AS engine, count(*) AS orders, sum(quantity) AS quantity
      FROM '{DBT_SILVER}/orders.parquet'
      UNION ALL
      SELECT 'spark', count(*), sum(quantity)
      FROM '{DUCK_BASE}/silver/spark/orders.parquet/*.parquet'
    """)
    duck(f"""
      SELECT 'dbt' AS engine, count(*) AS users
      FROM '{DBT_SILVER}/users.parquet'
      UNION ALL
      SELECT 'spark', count(*)
      FROM '{DUCK_BASE}/silver/spark/users.parquet/*.parquet'
    """)
    duck(f"DESCRIBE SELECT * FROM '{DBT_SILVER}/orders.parquet'")
    duck(f"DESCRIBE SELECT * FROM '{DUCK_BASE}/silver/spark/orders.parquet/*.parquet'")
else:
    print(
        f"dbt output not found in {DBT_SILVER}: run the dbt lab first (or set DBT_SILVER_DIR) "
        "to run the dbt-vs-Spark comparison. Showing only the Spark schema:"
    )
    duck(f"DESCRIBE SELECT * FROM '{DUCK_BASE}/silver/spark/orders.parquet/*.parquet'")

first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_ordered_at"))
users_flagged_df = (
    stg_users
    .join(first_orders, on="user_id", how="left")
    .withColumn(
        "birthdate_is_valid",
        F.coalesce(F.col("birthdate") <= F.to_date("first_ordered_at"), F.lit(True)),
    )
    .drop("first_ordered_at")
)

stg_orders.createOrReplaceTempView("stg_orders")
users_flagged_sql = spark.sql("""
    SELECT u.*,
           coalesce(u.birthdate <= to_date(f.first_ordered_at), true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (
        SELECT user_id, min(ordered_at) AS first_ordered_at
        FROM stg_orders
        GROUP BY user_id
    ) f ON u.user_id = f.user_id
""")

print("Ex1 DataFrame API - users with birthdate_is_valid = False:", users_flagged_df.filter(~F.col("birthdate_is_valid")).count())
print("Ex1 Spark SQL     - users with birthdate_is_valid = False:", users_flagged_sql.filter(~F.col("birthdate_is_valid")).count())
assert users_flagged_df.count() == users_flagged_sql.count() == stg_users.count()
assert set(users_flagged_df.columns) == set(users_flagged_sql.columns)
assert users_flagged_df.select(*users_flagged_sql.columns).exceptAll(users_flagged_sql).count() == 0
assert users_flagged_sql.exceptAll(users_flagged_df.select(*users_flagged_sql.columns)).count() == 0
users_flagged_df.filter(~F.col("birthdate_is_valid")).select(
    "user_id", "birthdate", "birthdate_is_valid"
).show(5, truncate=False)

addresses = users_bronze.select(F.element_at(F.split("address", "\n"), 2).alias("address_line_2"))

@F.udf("string")
def zip_code_udf(address_line_2):
    m = re.search(r"([A-Z]{2}) ([0-9]{5})$", address_line_2 or "")
    return m.group(2) if m else ""

with_regexp = addresses.select(
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code")
)
with_udf = addresses.select(zip_code_udf("address_line_2").alias("zip_code"))

import pandas as pd

@F.pandas_udf("string")
def zip_code_pandas(s: pd.Series) -> pd.Series:
    return s.str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")

pandas_variant = addresses.select(zip_code_pandas("address_line_2").alias("zip_code"))

def time_it(label, df, repeat=5):
    df.write.format("noop").mode("overwrite").save()
    t0 = time.perf_counter()
    for _ in range(repeat):
        df.write.format("noop").mode("overwrite").save()
    print(f"{label:<28} {(time.perf_counter() - t0) / repeat * 1000:9.1f} ms / run")

time_it("regexp_extract (native)", with_regexp)
time_it("plain Python UDF", with_udf)
time_it("pandas_udf (Arrow)", pandas_variant)
assert with_udf.exceptAll(with_regexp).count() == 0
assert pandas_variant.exceptAll(with_regexp).count() == 0

def run_downstream(users_df, orders_df):
    users_df.count()
    orders_df.count()
    orders_df.groupBy("product").count().collect()

t0 = time.perf_counter()
run_downstream(users_bronze, orders_bronze)
run_downstream(users_bronze, orders_bronze)
print(f"Ex3 without cache: 2 runs = {time.perf_counter() - t0:.3f} s")

users_bronze = users_bronze.cache()
orders_bronze = orders_bronze.cache()
t0 = time.perf_counter()
run_downstream(users_bronze, orders_bronze)
t_first = time.perf_counter() - t0
t0 = time.perf_counter()
run_downstream(users_bronze, orders_bronze)
t_second = time.perf_counter() - t0
print(f"Ex3 with cache:    1st run = {t_first:.3f} s (fills cache), 2nd run = {t_second:.3f} s")
if os.environ.get("PAUSE_FOR_UI") == "1":
    input("[Spark UI] Check http://localhost:4040 (Storage tab + stages 'skipped'), then press Enter...")
users_bronze.unpersist()
orders_bronze.unpersist()

if not SKIP_ICEBERG:
    first_user_id = stg_users.first()["user_id"]
    new_order = spark.createDataFrame(
        [("11111111-1111-4111-8111-111111111111", first_user_id, datetime(2026, 1, 1, 12, 0, 0), 3, "croissant")],
        schema=stg_orders.schema,
    )
    new_order.writeTo("lab.silver.orders").append()

    snapshots = spark.sql(
        "SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at"
    )
    snapshots.show(truncate=False)
    ids = [r["snapshot_id"] for r in snapshots.collect()]
    before_id = ids[-2]

    print("Current table      :", spark.table("lab.silver.orders").count(), "rows")
    print(
        "VERSION AS OF (old):",
        spark.sql(f"SELECT * FROM lab.silver.orders VERSION AS OF {before_id}").count(),
        "rows",
    )

    parquet_path = f"{BASE}/silver/spark/orders_exercise.parquet"
    stg_orders.write.mode("overwrite").parquet(parquet_path)
    parquet_before_count = spark.read.parquet(parquet_path).count()
    new_order.write.mode("append").parquet(parquet_path)
    print("Parquet rows before append:", parquet_before_count)
    print("Parquet rows after append:", spark.read.parquet(parquet_path).count())
    sync_warehouse()

spark.stop()
