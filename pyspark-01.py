# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/samples/data/mllib")

# COMMAND ----------

'dbfs:/databricks-datasets/samples/data/mllib/sample_tree_data.csv'

# COMMAND ----------

spark.read.format('csv').load('dbfs:/databricks-datasets/samples/data/mllib/sample_tree_data.csv').display()

# COMMAND ----------

df = spark.read.format('csv').load('dbfs:/databricks-datasets/samples/data/mllib/sample_tree_data.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('csv').save('/tmp/test.csv')

# COMMAND ----------


d1 = [(243, 'siva' , 5000) , (244, 'sankar' , 5000) , (245, 'kandula' , 5000) , (246, 'aruna' , 5000)]
s1 = ['eno' , 'ename' , 'esal']
df = spark.createDataFrame(d1, s1)

# COMMAND ----------

spark.read.format('csv').load('/tmp/test.csv').display()

# COMMAND ----------

df.write.format('csv').option('header' , 'true').mode('overwrite').save('/tmp/test.csv')

# COMMAND ----------

d1 = [(243, 'siva' , 5000) , (244, 'sankar' , 5000) , (245, 'kandula' , 5000) , (246, 'aruna' , 5000)]
s1 = ['eno' , 'ename' , 'esal']
df_1 = spark.createDataFrame(d1, s1)

# COMMAND ----------

df_1.write.format('csv').save('/tmp/test_1.csv')

# COMMAND ----------

df_1.write.format('csv').mode('error').save('/tmp/test_2.csv')

# COMMAND ----------

df_1.write.format('csv').mode('append').save('/tmp/test_2.csv')

# COMMAND ----------

df_1.write.format('csv').mode('overwrite').save('/tmp/test_2.csv')

# COMMAND ----------

df_1.write.format('csv').save('/tmp/test_2.csv')

# COMMAND ----------

spark.read.format('csv').load('/tmp/test_2.csv').display()

# COMMAND ----------

spark.read.format('csv').load('/tmp/test_2.csv').display()

# COMMAND ----------

spark.read.format().load()

# COMMAND ----------

d1 = [(243, 'siva' , 5000) , (244, 'sankar' , 5000) , (245, 'kandula' , 5000) , (246, 'aruna' , 5000)]
s1 = ['eno' , 'ename' , 'esal']
df_2 = spark.createDataFrame(d1, s1)

# COMMAND ----------

df_2.write.format('csv').option('header', 'true').save('tmp/test_3')

# COMMAND ----------



# COMMAND ----------

df_2.write.format('csv').mode('error').option('header', 'true').save('tmp/test_3')

# COMMAND ----------

df_2.write.format('csv').mode('append').option('header', 'true').save('tmp/test_3')

# COMMAND ----------

df_2.write.format('csv').mode('overwrite').option('header', 'true').save('tmp/test_3')

# COMMAND ----------

df_2.display()

# COMMAND ----------

spark.read.format('csv').option('header' , 'true').load('dbfs:/tmp/test_3').display()

# COMMAND ----------

# Example for reading CSV with different modes

# Permissive mode (default)
df_permissive = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').option('mode', 'PERMISSIVE').load('dbfs:/tmp/test_3')
display(df_permissive)


# COMMAND ----------

schema = 

# COMMAND ----------

# Example for reading CSV with different modes

# Permissive mode (default)
df_permissive = spark.read.format('csv').option('header', 'true').option('mode', 'PERMISSIVE').load('dbfs:/tmp/test_3')
display(df_permissive)

# COMMAND ----------

# Failfast mode
df_failfast = spark.read.format('csv').option('header', 'true').option('mode', 'FAILFAST').load('dbfs:/tmp/test_3')
display(df_failfast)

# COMMAND ----------




# Dropmalformed mode
df_dropmalformed = spark.read.format('csv').option('header', 'true').option('mode', 'DROPMALFORMED').load('dbfs:/tmp/test_3')
display(df_dropmalformed)

# COMMAND ----------



# COMMAND ----------

d1 = [(243, 'siva' , 5000) , (244, 'sankar' , 5000) , (245, 'kandula' , 5000) , (246, 'aruna' , 5000)]
s1 = ['eno' , 'ename' , 'esal']
df_2 = spark.createDataFrame(d1, s1)

# COMMAND ----------


