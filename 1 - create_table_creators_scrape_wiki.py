# Databricks notebook source
# MAGIC %md
# MAGIC # 1 - create_table_creators_scrape_wiki
# MAGIC Le o arquivo wiki_pages.json.gz e cria a tabela delta default.creators_scrape_wiki

# COMMAND ----------

file_path = "/Volumes/workspace/default/winnin/wiki_pages.json.gz"

df = spark.read.json(file_path)

df.printSchema()
display(df)

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.creators_scrape_wiki")

# COMMAND ----------

display(spark.sql("SELECT * FROM default.creators_scrape_wiki"))
