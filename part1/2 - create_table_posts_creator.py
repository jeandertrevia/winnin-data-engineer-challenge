# Databricks notebook source
# MAGIC %md
# MAGIC # 2 - create_table_posts_creator
# MAGIC Le o arquivo posts_creator.json.gz e cria a tabela delta default.posts_creator

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, col

file_path = "/Volumes/workspace/default/winnin/posts_creator.json.gz"

df = spark.read.json(file_path) \
    .withColumn("published_at", from_unixtime(col("published_at")).cast("timestamp"))

df.printSchema()
display(df)

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.posts_creator")

# COMMAND ----------

display(spark.sql("SELECT * FROM default.posts_creator"))
