# Databricks notebook source
# MAGIC %md [Enabling Azure AD Credential Passthrough to Azure Data Lake Storage Gen1](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html#access-adls-automatically-with-your-aad-credentials)
# MAGIC
# MAGIC Set permissions for your data
# MAGIC When users run notebooks that access Azure Data Lake Storage Gen1 using credential passthrough, all of the data they access must be stored entirely in Azure Data Lake Storage Gen1. Credential passthrough does not support filesystems other than Azure Data Lake Storage Gen1.
# MAGIC
# MAGIC Make sure that user permissions are set correctly for their data. The Azure Active Directory user who logs into Azure Databricks should be able to read (and, if necessary, write) their Azure Data Lake Storage Gen1 data.
# MAGIC
# MAGIC ![img](https://docs.microsoft.com/en-us/azure/data-lake-store/media/data-lake-store-secure-data/adl.acl.2.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **To enable Azure Data Lake Storage credential passthrough for a cluster:**
# MAGIC
# MAGIC * When you create the cluster, set the Cluster Mode to High Concurrency.
# MAGIC * Use Databricks Runtime 5.1 or above.
# MAGIC * Select Enable credential passthrough and only allow Python and SQL commands.
# MAGIC
# MAGIC ![img](https://docs.azuredatabricks.net/_images/adls-credential-passthrough.png)

# COMMAND ----------

# DBTITLE 1,Access allowed folder on ADLS via path using AAD token - it works (as expected)
booksDf = spark.read.format("delta").load("adl://sreeramwrkshpadl.azuredatalakestore.net/gwsroot/books")

# COMMAND ----------

display(booksDf)

# COMMAND ----------

# DBTITLE 1,Access another allowed folder on ADLS via path using AAD token - it works (as expected)
prqDf = spark.read.parquet("adl://sreeramwrkshpadl.azuredatalakestore.net/gwsroot/books-prq")

# COMMAND ----------

display(prqDf)

# COMMAND ----------

# DBTITLE 1,Access to Blob FS directly - doesn't work (as expected), but error should be different
df4 = spark.read.text("wasbs://raw@sreeramwrkshpdb.blob.core.windows.net/")
