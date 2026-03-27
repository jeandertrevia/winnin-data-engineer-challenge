# Databricks notebook source
# MAGIC %md
# MAGIC # 4 - analyze_creators
# MAGIC Analises dos creators usando join entre default.users_yt e default.posts_creator

# COMMAND ----------

from pyspark.sql.functions import (
    col, rank, count, date_format, months_between, max as spark_max,
    coalesce, lit, explode, lower, trim
)
from pyspark.sql.window import Window

# COMMAND ----------

users_yt = spark.table("default.users_yt")
posts    = spark.table("default.posts_creator")

ref_date = posts.agg(spark_max("published_at")).collect()[0][0]
print(f"Data de referencia: {ref_date}")

posts_joined = posts.join(users_yt, posts.yt_user == users_yt.user_id, "inner")

posts_6m = posts_joined.filter(
    months_between(lit(ref_date), col("published_at")) <= 6
)
print(f"Posts nos ultimos 6 meses: {posts_6m.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 3 posts por likes de cada creator (ultimos 6 meses)

# COMMAND ----------

window_likes = Window.partitionBy("user_id").orderBy(col("likes").desc())

top3_likes = (
    posts_6m
    .withColumn("rank", rank().over(window_likes))
    .filter(col("rank") <= 3)
    .select("user_id", "title", "likes", "rank")
    .orderBy("user_id", "rank")
)

display(top3_likes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 3 posts por views de cada creator (ultimos 6 meses)

# COMMAND ----------

window_views = Window.partitionBy("user_id").orderBy(col("views").desc())

top3_views = (
    posts_6m
    .withColumn("rank", rank().over(window_views))
    .filter(col("rank") <= 3)
    .select("user_id", "title", "views", "rank")
    .orderBy("user_id", "rank")
)

display(top3_views)

# COMMAND ----------

# MAGIC %md
# MAGIC ## yt_user em posts_creator que nao estao em users_yt

# COMMAND ----------

not_in_users_yt = (
    posts.select("yt_user").distinct()
    .join(users_yt, posts.yt_user == users_yt.user_id, "left_anti")
    .orderBy("yt_user")
)

display(not_in_users_yt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publicacoes por mes de cada creator

# COMMAND ----------

pubs_por_mes = (
    posts_joined
    .withColumn("mes", date_format(col("published_at"), "yyyy/MM"))
    .groupBy("user_id", "mes")
    .agg(count("*").alias("publicacoes"))
    .orderBy("user_id", "mes")
)

display(pubs_por_mes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra 1 - Publicacoes por mes com 0 nos meses sem video

# COMMAND ----------

months_df = spark.sql("""
    SELECT date_format(mes, 'yyyy/MM') AS mes
    FROM (
        SELECT explode(sequence(
            date_trunc('month', MIN(published_at)),
            date_trunc('month', MAX(published_at)),
            INTERVAL 1 MONTH
        )) AS mes
        FROM default.posts_creator
    )
""")

all_combinations = users_yt.select("user_id").filter(col("user_id").isNotNull()) \
    .crossJoin(months_df)

pubs_com_zero = (
    all_combinations
    .join(pubs_por_mes, ["user_id", "mes"], "left")
    .fillna(0, subset=["publicacoes"])
    .orderBy("user_id", "mes")
)

display(pubs_com_zero)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra 2 - Publicacoes por mes no formato pivotado (user_id x mes)

# COMMAND ----------

meses = [row["mes"] for row in months_df.orderBy("mes").collect()]

pivot_df = (
    pubs_com_zero
    .groupBy("user_id")
    .pivot("mes", meses)
    .agg({"publicacoes": "sum"})
    .fillna(0)
    .orderBy("user_id")
)

display(pivot_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra 3 - Top 3 tags mais utilizadas por creator

# COMMAND ----------

window_tags = Window.partitionBy("user_id").orderBy(col("qtd").desc())

top3_tags = (
    posts_joined
    .select("user_id", explode(col("tags")).alias("tag"))
    .withColumn("tag", lower(trim(col("tag"))))
    .groupBy("user_id", "tag")
    .agg(count("*").alias("qtd"))
    .withColumn("rank", rank().over(window_tags))
    .filter(col("rank") <= 3)
    .select("user_id", "tag", "qtd", "rank")
    .orderBy("user_id", "rank")
)

display(top3_tags)
