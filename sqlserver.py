# Databricks notebook source
spark.conf.set("fs.azure.account.key.strgacntpgl1.blob.core.windows.net","2xBVsEoDpK9DIb04CLbWw8UU+fWQqrhSw4Z+LmCxwsMe0u4nIBp1hyVt7xd3nWwHaKcg9uAH6SLFvtpQtJMPBw==")

# COMMAND ----------

df1 = spark.read.format("csv").options(header='True').load("wasbs://sqlserver@strgacntpgl1.blob.core.windows.net/loadoutput/Final.csv")
df2 = spark.read.format("csv").options(header='True').load("wasbs://sqlserver@strgacntpgl1.blob.core.windows.net/refined/fulldata.csv")
display(df2)

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC df2 = df2.union(df1)
# MAGIC df2 = df2.union(df1) 
# MAGIC # we do union two times so that we get duplicates
# MAGIC df2.show()

# COMMAND ----------

# we are removing duplicates here

from pyspark.sql.functions import*
df2 = df2.groupby('PersonID','Name').agg(max('LastModifytime').alias('LastModifytime'))

# COMMAND ----------

df2.show()

# COMMAND ----------

url="jdbc:sqlserver://serverdbgl1.database.windows.net:1433;database=dbgl1;user=dbuser@serverdbgl1;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

properties = {

 "user" : "dbuser",

 "password" : "Sai.1234" }

# COMMAND ----------

from pyspark.sql import *
import pandas as pd
finaldf = DataFrameWriter(df2)
finaldf.jdbc(url=url,table="final",mode="overwrite",properties=properties)

# COMMAND ----------


