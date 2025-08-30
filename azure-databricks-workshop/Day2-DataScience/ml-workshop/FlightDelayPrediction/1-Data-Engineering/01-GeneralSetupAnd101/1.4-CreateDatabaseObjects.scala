// Databricks notebook source
// MAGIC %md
// MAGIC # What's in this exercise?
// MAGIC
// MAGIC 1) Database definition<BR> 
// MAGIC 2) External remote JDBC table definition

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Create the flight_db database

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.1. Create database

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS flight_db;

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1.2. Validate

// COMMAND ----------

spark.catalog.listDatabases.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Create external table definition for a remote Azure SQL database table over JDBC

// COMMAND ----------

// MAGIC %md Get database credentials from secrets setup from the previous workshop

// COMMAND ----------

val jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")

// COMMAND ----------

//replace with your server and database names
val jdbcHostname = "srwrkshpsqlserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "sreeramWrkshpDb"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

// MAGIC %sql
// MAGIC --This table is in Azure SQL Database
// MAGIC DROP TABLE IF EXISTS flight_db.predictions_needed;
// MAGIC CREATE TABLE flight_db.predictions_needed
// MAGIC USING org.apache.spark.sql.jdbc
// MAGIC OPTIONS (
// MAGIC   url 'jdbc:sqlserver://demodbsrvr.database.windows.net:1433;database=sreeramWrkshpDb',
// MAGIC   dbtable 'predictions_needed',
// MAGIC   user "<jdbcUsername>",
// MAGIC   password "<jdbcPassword>"
// MAGIC )

// COMMAND ----------


