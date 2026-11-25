# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # PySpark Deep Dive — Complete Hands-On Notebook
# MAGIC
# MAGIC **Week 03 · S06 · Codebasics Data Engineering Bootcamp**
# MAGIC
# MAGIC Run this notebook top-to-bottom in a Databricks Free Edition workspace.
# MAGIC
# MAGIC ### Before you start
# MAGIC 1. Upload `orders.csv`, `restaurants.csv`, `customers.csv` to a Unity Catalog Volume.
# MAGIC 2. Update the `BASE` path in the first code cell to point at your Volume.
# MAGIC 3. Attach this notebook to **Serverless** compute.
# MAGIC 4. Run cells top-to-bottom.
# MAGIC
# MAGIC ### What's in here
# MAGIC - **Sections 0–2** — setup and inspection (mandatory)
# MAGIC - **Sections 3–10** — the 9 PySpark commands you'll use every day + Spark SQL
# MAGIC - **Section 11** — peek at internals (`.explain()`, partitions, laziness)
# MAGIC - **Sections 12–15** — performance: broadcast joins, partitioning, caching, skew
# MAGIC - **Section 16** — cheat sheet + common gotchas
# MAGIC
# MAGIC Every code cell has inline comments. Read them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. One-time setup
# MAGIC
# MAGIC Run these two cells first. Everything below assumes the three DataFrames exist.

# COMMAND ----------

# ── Point BASE at YOUR Volume path (change if different) ─────────
BASE = "/Volumes/workspace/default/cbvolume"

# ── Confirm SparkSession is alive ───────────────────────────────
# On Databricks Free Edition, `spark` is pre-created — you don't
# import or instantiate it. Just use it.
spark

# COMMAND ----------

# Spark version — Free Edition auto-upgrades to the latest DBR
print(spark.version)

# COMMAND ----------

# ── Load the three CSVs ─────────────────────────────────────────
orders_df = (spark.read
    .option("header", True)          # first row is column names
    .option("inferSchema", True)     # let Spark guess types (dev only)
    .csv(f"{BASE}/orders.csv"))

restaurants_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE}/restaurants.csv"))

customers_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE}/customers.csv"))

# Sanity check — counts should match: 10000 / 200 / 2000
print(f"orders: {orders_df.count()}, "
      f"restaurants: {restaurants_df.count()}, "
      f"customers: {customers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reading data — `spark.read`
# MAGIC
# MAGIC Alternative read patterns you'll use in production.
# MAGIC
# MAGIC > ⚠️ `inferSchema=True` triggers an extra scan just to guess types. Fine for a
# MAGIC > 500 KB CSV. Never do this on multi-GB files — pass an explicit schema instead.

# COMMAND ----------

# ── Production-style read with an EXPLICIT schema ──────────────
# One scan only (no inference). Deterministic types. Recommended for prod.
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, DateType,
)

orders_schema = StructType([
    StructField("order_id",      StringType(), nullable=False),
    StructField("customer_id",   StringType(), nullable=False),
    StructField("restaurant_id", StringType(), nullable=False),
    StructField("order_date",    DateType(),   nullable=True),
    StructField("city",          StringType(), nullable=True),
    StructField("amount",        DoubleType(), nullable=True),
    StructField("status",        StringType(), nullable=True),
])

orders_df_prod = (spark.read
    .option("header", True)
    .schema(orders_schema)             # no inference — one scan only
    .csv(f"{BASE}/orders.csv"))

orders_df_prod.printSchema()

# COMMAND ----------

# ── Reading other formats (for reference — don't run unless files exist) ──
# Parquet — columnar, splittable, compressed, no options needed
# df = spark.read.parquet(f"{BASE}/some_parquet_folder")

# Delta — the Databricks default table format
# df = spark.read.format("delta").load(f"{BASE}/some_delta_folder")

# From a Unity Catalog table
# df = spark.table("workspace.default.orders")

# JSON (one JSON object per line)
# df = spark.read.json(f"{BASE}/events.json")

print("Reference cell — see comments.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inspecting a DataFrame
# MAGIC
# MAGIC The **four commands** you run on any new DataFrame.

# COMMAND ----------

# Schema (columns + types)
orders_df.printSchema()

# COMMAND ----------

# First 5 rows. Default is 20. Use truncate=False for long strings.
orders_df.show(5)

# COMMAND ----------

# Total row count. ⚠️ This is an ACTION — triggers a Spark Job.
orders_df.count()

# COMMAND ----------

# Summary stats for numeric columns
orders_df.describe().show()

# COMMAND ----------

# Other inspection helpers
print("Columns:", orders_df.columns)
print("Types  :", orders_df.dtypes)
print("N cols :", len(orders_df.columns))

# head / first / take — small results only
print("First row:", orders_df.first())
print("First 3  :", orders_df.head(3))

# ⚠️ .collect() brings ALL rows to the driver. Never use on big data.
# Fine here since restaurants is 200 rows:
print(f"Total restaurants collected: {len(restaurants_df.collect())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Selecting columns — `select`
# MAGIC
# MAGIC The PySpark equivalent of SQL's `SELECT` clause.
# MAGIC
# MAGIC **Why `col()` over strings?** Strings work for simple selects. `col()` lets you
# MAGIC write expressions, comparisons, and function calls. Get in the habit early.

# COMMAND ----------

from pyspark.sql.functions import col

# ── Pass column names as strings ───────────
orders_df.select("order_id", "city", "amount").show(5)

# COMMAND ----------

# ── Pass col() objects — recommended in production ──
# col() returns a Column expression you can compose (math, comparisons, funcs).
orders_df.select(col("order_id"), col("amount")).show(5)

# COMMAND ----------

# ── Rename inline with .alias() ──
orders_df.select(col("city").alias("order_city")).show(5)

# COMMAND ----------

# ── Do math right inside select ──
from pyspark.sql.functions import round as F_round

orders_df.select(
    col("order_id"),
    col("amount"),
    F_round(col("amount") * 0.18, 2).alias("gst_amount"),   # 18% GST
).show(5)

# COMMAND ----------

# ── Drop columns (opposite of select) ──
print(orders_df.drop("status").columns)
# Returns a new DataFrame WITHOUT the status column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Filtering rows — `filter` / `where`
# MAGIC
# MAGIC `filter` and `where` are **identical** — same method, two names (for SQL folks).
# MAGIC
# MAGIC ⚠️ **The #1 beginner gotcha**: use `&` (AND) and `|` (OR), NOT Python's `and`/`or`.
# MAGIC And wrap each condition in parentheses.

# COMMAND ----------

from pyspark.sql.functions import col

# ── Single condition ──
orders_df.filter(col("city") == "Bengaluru").show(5)

# COMMAND ----------

# ── Multiple conditions — use & and | with parens ──
orders_df.filter(
    (col("city") == "Bengaluru") & (col("amount") > 500)
).show(5)

# COMMAND ----------

# ── OR condition ──
orders_df.filter(
    (col("status") == "cancelled") | (col("status") == "placed")
).count()

# COMMAND ----------

# ── NOT — use ~ ──
orders_df.filter(~(col("status") == "delivered")).count()

# COMMAND ----------

# ── .where() is identical to .filter() (SQL-friendly alias) ──
orders_df.where(col("status") == "delivered").count()

# COMMAND ----------

# ── isin — SQL's IN clause ──
orders_df.filter(
    col("city").isin("Bengaluru", "Hyderabad", "Mumbai")
).count()

# COMMAND ----------

# ── isNull / isNotNull ──
print("Non-null status:", orders_df.filter(col("status").isNotNull()).count())
print("Null amount   :", orders_df.filter(col("amount").isNull()).count())

# COMMAND ----------

# ── between (inclusive on both ends) ──
orders_df.filter(col("amount").between(500, 1000)).count()

# COMMAND ----------

# ── like (SQL LIKE) and rlike (regex) ──
restaurants_df.filter(col("name").like("Meghana%")).show()
restaurants_df.filter(col("name").rlike(".*Koramangala.*")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Creating derived columns — `withColumn`
# MAGIC
# MAGIC Adds a new column **or overwrites** an existing one.

# COMMAND ----------

from pyspark.sql.functions import col, when, lit

# ── Simple derived column (math) ──
orders_df = orders_df.withColumn(
    "amount_with_gst",
    col("amount") * 1.18                # 18% GST
)

orders_df.select("order_id", "amount", "amount_with_gst").show(5)

# COMMAND ----------

# ── Conditional column (SQL's CASE WHEN) ──
orders_df = orders_df.withColumn(
    "value_tier",
    when(col("amount") > 1500, "premium")
    .when(col("amount") > 500,  "standard")
    .otherwise("budget")
)

orders_df.select("order_id", "amount", "value_tier").show(10)

# COMMAND ----------

# ── Constant column — same value for every row ──
orders_df = orders_df.withColumn("source", lit("Swiggy"))

orders_df.select("order_id", "source").show(3)

# COMMAND ----------

# ── Type casting ──
orders_df.withColumn(
    "amount_int",
    col("amount").cast("int")           # truncates decimals
).select("amount", "amount_int").show(5)

# COMMAND ----------

# ── Renaming a column ──
tmp = orders_df.withColumnRenamed("amount", "order_amount")
print("Renamed columns:", tmp.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date functions worth knowing

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek, date_format

orders_dates = (orders_df
    .withColumn("order_year",  year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
    .withColumn("day_of_week", dayofweek(col("order_date")))
    .withColumn(
        "month_name",
        date_format(col("order_date"), "MMMM yyyy")
    )
)

orders_dates.select(
    "order_id", "order_date",
    "order_year", "order_month", "day_of_week", "month_name",
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Aggregations — `groupBy` + `agg`
# MAGIC
# MAGIC The workhorse for analytics. SQL's `GROUP BY`.

# COMMAND ----------

# ── Simple: one column, one aggregation ──
orders_df.groupBy("city").count().show()
# Column "count" is added automatically.

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count, min, max, countDistinct, col

# ── Multiple aggregations at once — the common pattern ──
city_revenue = (orders_df
    .filter(col("status") == "delivered")
    .groupBy("city")
    .agg(
        count("order_id").alias("orders"),
        sum("amount").alias("revenue"),
        avg("amount").alias("avg_order"),
        max("amount").alias("biggest_order"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy(col("revenue").desc())
)

city_revenue.show()

# COMMAND ----------

# ── Group by multiple columns ──
(orders_df
    .groupBy("city", "status")
    .agg(count("order_id").alias("orders"))
    .orderBy("city", "status")
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sorting: `orderBy` + `asc` / `desc`

# COMMAND ----------

from pyspark.sql.functions import col

# Descending
city_revenue.orderBy(col("revenue").desc()).show()

# Ascending (default)
city_revenue.orderBy("revenue").show()

# Multi-column sort
city_revenue.orderBy(col("orders").desc(), col("avg_order").desc()).show()

# .limit(n) — take top n
city_revenue.orderBy(col("revenue").desc()).limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Joins
# MAGIC
# MAGIC Combine two DataFrames on a common key.

# COMMAND ----------

# ── Inner join — only rows that match on both sides ──
enriched = orders_df.join(
    restaurants_df,
    on="restaurant_id",
    how="inner"
)

enriched.select(
    "order_id", "amount",
    "name", "cuisine", "rating"
).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The five join types
# MAGIC
# MAGIC - `inner`     — only matches
# MAGIC - `left`      — all from left, NULLs from right where no match
# MAGIC - `right`     — mirror of left
# MAGIC - `outer`     — everything from both, NULLs where missing
# MAGIC - `left_anti` — rows from LEFT that have NO match in right (find gaps!)

# COMMAND ----------

# ── Left join — keep every order, even if restaurant missing ──
orders_df.join(restaurants_df, "restaurant_id", "left").show(5)

# COMMAND ----------

# ── Outer join — full picture ──
orders_df.join(restaurants_df, "restaurant_id", "outer").count()

# COMMAND ----------

# ── left_anti — find orphaned orders (should be 0 in our clean data) ──
orphans = orders_df.join(restaurants_df, "restaurant_id", "left_anti")
print(f"Orphaned orders: {orphans.count()}")

# COMMAND ----------

# ── Multi-column join ──
customers_df.join(
    orders_df,
    on=["customer_id", "city"],
    how="inner"
).show(5)

# COMMAND ----------

# ── Three-way join ──
from pyspark.sql.functions import col

full = (orders_df
    .join(restaurants_df, "restaurant_id")
    .join(customers_df,   "customer_id")
)

full.select(
    "order_id",
    orders_df["city"].alias("order_city"),
    restaurants_df["name"].alias("restaurant"),
    customers_df["name"].alias("customer"),
    "amount",
).show(5)

# COMMAND ----------

# ── When column names collide (orders, restaurants, customers all have 'city') ──
# Fix by aliasing DataFrames and qualifying columns.
from pyspark.sql.functions import col

result = orders_df.alias("o").join(
    restaurants_df.alias("r"),
    col("o.restaurant_id") == col("r.restaurant_id"),
)

result.select(
    col("o.order_id"),
    col("o.city").alias("order_city"),
    col("r.city").alias("restaurant_city"),
).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Spark SQL
# MAGIC
# MAGIC Register DataFrames as SQL tables, then run plain SQL.
# MAGIC
# MAGIC **Big idea:** DataFrame API and SQL run through the exact same Catalyst optimizer.
# MAGIC Same plan, same speed. Use whichever reads better for the team.

# COMMAND ----------

# ── Register temp views (session-scoped) ──
orders_df.createOrReplaceTempView("orders")
restaurants_df.createOrReplaceTempView("restaurants")
customers_df.createOrReplaceTempView("customers")

print("Registered: orders, restaurants, customers")

# COMMAND ----------

# ── Run SQL — the full ANSI SQL surface is available ──
top_cuisines = spark.sql("""
    SELECT
        r.cuisine,
        COUNT(*)                    AS num_orders,
        SUM(o.amount)               AS revenue,
        ROUND(AVG(o.amount), 2)     AS avg_order
    FROM orders o
    JOIN restaurants r USING (restaurant_id)
    WHERE o.status = 'delivered'
    GROUP BY r.cuisine
    ORDER BY revenue DESC
""")

top_cuisines.show()

# COMMAND ----------

# ── Mix SQL and DataFrame API mid-query ──
from pyspark.sql.functions import col

# Start with SQL
step1 = spark.sql("""
    SELECT restaurant_id, SUM(amount) AS revenue
    FROM orders WHERE status = 'delivered'
    GROUP BY restaurant_id
""")

# Continue with DataFrame API
step2 = (step1
    .join(restaurants_df, "restaurant_id")
    .select("name", "cuisine", "city", "revenue")
    .orderBy(col("revenue").desc())
    .limit(10)
)
step2.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus — window functions (top N per group)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, sum as F_sum

# Rank restaurants by revenue WITHIN each city — top 3 per city
w = Window.partitionBy("city").orderBy(col("revenue").desc())

top3_per_city = (orders_df
    .filter(col("status") == "delivered")
    .groupBy("restaurant_id", "city")
    .agg(F_sum("amount").alias("revenue"))
    .withColumn("rank_in_city", row_number().over(w))
    .filter(col("rank_in_city") <= 3)
    .orderBy("city", "rank_in_city")
)

top3_per_city.show(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Writing data
# MAGIC
# MAGIC `.write` is an ACTION — it triggers execution.
# MAGIC
# MAGIC `mode` values:
# MAGIC - `overwrite` — replace what was there
# MAGIC - `append` — add to existing
# MAGIC - `error` — fail if exists (default)
# MAGIC - `ignore` — do nothing if exists

# COMMAND ----------

# ── As Parquet (file) ──
city_revenue.write.mode("overwrite").parquet(f"{BASE}/output/city_revenue_parquet")
print(f"Written to {BASE}/output/city_revenue_parquet")

# COMMAND ----------

# ── As Delta table (Databricks-preferred) ──
# ⚠️ Uses your Unity Catalog — adjust catalog.schema to match your workspace.
(city_revenue.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("workspace.default.city_revenue"))

print("Registered as workspace.default.city_revenue")

# COMMAND ----------

# ── Read the Delta table back ──
spark.table("workspace.default.city_revenue").show()

# COMMAND ----------

# ── Write partitioned by column (on-disk layout) ──
# ⚠️ partitionBy (write) = on-disk layout. .repartition() = in-memory parallelism.
#     Same word — different concepts. See section 13.
(orders_df.write
    .mode("overwrite")
    .partitionBy("city")
    .format("delta")
    .saveAsTable("workspace.default.orders_by_city"))

print("Partitioned Delta table created — one folder per city.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. The chained pipeline
# MAGIC
# MAGIC Real PySpark: one readable chain, one action at the end. **Nothing runs until `.show()`.**

# COMMAND ----------

from pyspark.sql.functions import col, sum as F_sum

top5_cuisines = (orders_df
    .filter(col("status") == "delivered")
    .join(restaurants_df, "restaurant_id")
    .filter(col("rating") >= 4.0)              # only top-rated restaurants
    .groupBy("cuisine")
    .agg(F_sum("amount").alias("revenue"))
    .orderBy(col("revenue").desc())
    .limit(5)
)

top5_cuisines.show()