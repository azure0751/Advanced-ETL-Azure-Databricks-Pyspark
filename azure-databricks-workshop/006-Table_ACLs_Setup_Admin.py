# Databricks notebook source
# MAGIC %md # Table ACLs Setup by an Admin

# COMMAND ----------

# MAGIC %md #### Stuff to be done to enable both table and row-level grants to a non-admin user

# COMMAND ----------

# DBTITLE 1,Show permissions for non-admin on Database - which is as expected "nothing"
# MAGIC %sql SHOW GRANT `snudurupati@outlook.com` ON DATABASE crimes_db

# COMMAND ----------

# DBTITLE 1,Grant select on country_codes table to non-admin
# MAGIC %sql GRANT SELECT ON TABLE crimes_db.chicago_crimes_raw to `snudurupati@outlook.com`

# COMMAND ----------

# DBTITLE 1,Show permissions for non-admin on country_codes table
# MAGIC %sql SHOW GRANT `snudurupati@outlook.com` ON TABLE crimes_db.chicago_crimes_raw

# COMMAND ----------

# DBTITLE 1,Create mid-west view over states table and grant select on that view to non-admin
# MAGIC %sql 
# MAGIC
# MAGIC CREATE OR REPLACE VIEW default.states_mw AS SELECT * FROM default.states where state_abbr in ('MI','IL','OH','SD','ND','IW');
# MAGIC GRANT SELECT ON VIEW default.states_mw to `snudurupati@outlook.com`;

# COMMAND ----------

# DBTITLE 1,Show permissions for non-admin on states_ne view
# MAGIC %sql SHOW GRANT `snudurupati@outlook.com` ON VIEW default.states_mw

# COMMAND ----------

# DBTITLE 1,Show permissions for non-admin on underlying states table - which is as expected "nothing"
# MAGIC %sql SHOW GRANT `snudurupati@outlook.com` ON TABLE default.states

# COMMAND ----------

# MAGIC %md #### Some stuff to be done for underlying table, if it was not created with Table ACLs "on"
# MAGIC * See the [FAQ](https://docs.azuredatabricks.net/administration-guide/admin-settings/table-acls/object-permissions.html#frequently-asked-questions)

# COMMAND ----------

# DBTITLE 1,Assign admin owner to underlying states table, as it was created without table acls
# MAGIC %sql ALTER TABLE default.states OWNER TO `sreeram.nudurupati@databricks.com`

# COMMAND ----------

# DBTITLE 1,Show permissions for admin on underlying states table
# MAGIC %sql SHOW GRANT `sreeram.nudurupati@databricks.com` ON TABLE default.states

# COMMAND ----------

# DBTITLE 1,Show permissions for admin on derived states_ne view - which is as expected
# MAGIC %sql SHOW GRANT `sreeram.nudurupati@databricks.com` ON VIEW default.states_mw
