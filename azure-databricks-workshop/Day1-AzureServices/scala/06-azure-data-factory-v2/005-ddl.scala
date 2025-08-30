// Databricks notebook source
//JDBC connectivity related
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
val jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")
val jdbcPort = 1433
val jdbcDatabase = "sreeramWrkshpDb"

//Replace with your server name
val jdbcHostname = "srwrkshpsqlserver.database.windows.net"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

import java.sql._
import java.util.Calendar

//Function to create table
def createTable(querySql: String): Unit = 
{
    var conn: Connection = null
    var stmt: Statement = null
  
    try {
        Class.forName(driverClass)
        conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

        val preparedStmt: PreparedStatement = conn.prepareStatement(querySql)
      
      //execute 
        preparedStmt.execute

        // cleanup
        preparedStmt.close
        conn.close
        println("Table created!")
    } catch {
        case se: SQLException => se.printStackTrace
        case e:  Exception => e.printStackTrace
    } finally {
        try {
            if (stmt!=null) stmt.close
        } catch {
            case se2: SQLException => // nothing we can do
        }
        try {
            if (conn!=null) conn.close
        } catch {
            case se: SQLException => se.printStackTrace
        } //end finally-try
    } //end try
}

// COMMAND ----------

//1. Create table: batch_job_history
val ddlquery1 = """
             DROP TABLE IF EXISTS dbo.BATCH_JOB_HISTORY; 
             CREATE TABLE BATCH_JOB_HISTORY( 
             batch_id int, 
             batch_step_id int, 
             batch_step_description varchar(100), 
             batch_step_status varchar(30), 
             batch_step_time varchar(30) );
            """.stripMargin

createTable(ddlquery1)

// COMMAND ----------

//2. Create table: chicago_crimes_count
val ddlquery2 = """
             DROP TABLE IF EXISTS dbo.CHICAGO_CRIMES_COUNT; 
             CREATE TABLE CHICAGO_CRIMES_COUNT( 
             case_type varchar(100), 
             crime_count bigint);
             """.stripMargin

createTable(ddlquery2)

// COMMAND ----------

//3. Create table: chicago_crimes_count_by_year
val ddlquery3 = """
             DROP TABLE IF EXISTS dbo.CHICAGO_CRIMES_COUNT_BY_YEAR; 
             CREATE TABLE CHICAGO_CRIMES_COUNT_YEAR( 
             case_year int,
             case_type varchar(100), 
             crime_count bigint);
             """.stripMargin

createTable(ddlquery3)
