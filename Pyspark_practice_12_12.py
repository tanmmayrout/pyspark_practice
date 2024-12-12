# Databricks notebook source


# Creating a DataFrame
data = [("Alice", 29), ("Bob", 31), ("Cathy", 23)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

# Filtering Data
df.filter(df.Age > 25).show()


# COMMAND ----------

data = [("Alice", 29), ("Bob", 31)]
df = spark.createDataFrame(data, ["Name", "Age"])


# Select specific columns
df.select("Name").show()

# Filter rows
df.filter(df.Age > 30).show()

# Adding a new column
df = df.withColumn("NewColumn", df.Age + 5)
df.show()

df.groupBy("Age").count().show()


data2 = [("Alice", "F"), ("Bob", "M")]
df2 = spark.createDataFrame(data2, ["Name", "Gender"])

df.join(df2, "Name").show()




# COMMAND ----------


