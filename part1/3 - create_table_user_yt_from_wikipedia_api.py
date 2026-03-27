# Databricks notebook source
# MAGIC %md
# MAGIC # 3 - create_table_user_yt_from_wikipedia_api
# MAGIC Busca na API da Wikipedia o user_id do YouTube para cada wiki_page
# MAGIC e cria a tabela delta default.users_yt

# COMMAND ----------

import requests
import re
from pyspark.sql import Row

# COMMAND ----------

wiki_pages = spark.sql("SELECT wiki_page FROM default.creators_scrape_wiki").collect()

# COMMAND ----------

def get_youtube_user_id(page_name):
    try:
        resp = requests.get(
            "https://en.wikipedia.org/w/api.php",
            params={"action": "parse", "page": f"{page_name}", "format": "json"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = resp.json()

        if "error" in data:
            return None

        html = data["parse"]["text"]["*"]

        m = re.search(r'youtube\.com/@([\w\-\.]+)', html)
        if m:
            return m.group(1)

        m = re.search(r'youtube\.com/user/([\w\-\.]+)', html)
        if m:
            return m.group(1)

        return None
    except Exception:
        return None

# COMMAND ----------

results = []
for row in wiki_pages:
    wiki_page = row["wiki_page"]
    user_id = get_youtube_user_id(wiki_page)
    results.append(Row(user_id=user_id, wiki_page=wiki_page))
    print(f"{wiki_page} -> {user_id}")

# COMMAND ----------

df = spark.createDataFrame(results)
display(df)

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.users_yt")

# COMMAND ----------

display(spark.sql("SELECT * FROM default.users_yt"))
