# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading FactSet Data
# MAGIC We'll be reading corporate filings from Factset data sharing as vector store capability for our agent. Please note that delta shares cannot be directly used into a vector table yet. Also, for the purpose of the demo, we'll be re-embedding FactSet content in order to remove dependency to external model (openAI). In real life scenario, vector index will be built atop of the existing embedding, providing embedding function to our agent

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Data being shared
foreign_catalog = 'factset_demo'
foreign_database = 'factset'
foreign_raw_table = 'filings_vectors'

# Where we'll store our vector index
home_catalog = 'financial_services'
home_database = 'investment_analytics'
home_raw_table = 'factset_filings'
home_vector_table = 'factset_filings_vectors'
home_vector_endpoint = 'dbdemos_vs_endpoint'

# COMMAND ----------

_ = sql(f"""
        CREATE OR REPLACE TABLE {home_catalog}.{home_database}.{home_raw_table} AS
        SELECT * FROM {foreign_catalog}.{foreign_database}.{foreign_raw_table}""")

_ = sql(f"""ALTER TABLE {home_catalog}.{home_database}.{home_raw_table} 
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)""")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc_client = VectorSearchClient()

# COMMAND ----------

def exist_vector_store():
  try:
    vsc_client.get_index(home_vector_endpoint, f"{home_catalog}.{home_database}.{home_vector_table}")
    return True
  except:
    return False

# COMMAND ----------

def create_vector_store_if_not_exist():
  if not exist_vector_store():
    vsc_client.create_delta_sync_index(
      endpoint_name=home_vector_endpoint,
      source_table_name=f"{home_catalog}.{home_database}.{home_raw_table}",
      index_name=f"{home_catalog}.{home_database}.{home_vector_table}",
      pipeline_type="TRIGGERED",
      primary_key="chunk_id",
      embedding_source_column="content",
      embedding_model_endpoint_name="databricks-gte-large-en"
    )
  return vsc_client.get_index(home_vector_endpoint, f"{home_catalog}.{home_database}.{home_vector_table}")

# COMMAND ----------

create_vector_store_if_not_exist()

# COMMAND ----------


