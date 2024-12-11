# Databricks notebook source
# MAGIC %md
# MAGIC ##PYSPARK TUTORIAL PART-1(BEGINEER)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data reading with JSON

# COMMAND ----------

df_json = spark.read.format('json') \
                    .option('inferSchema',True) \
                    .option('headers',True) \
                    .option('multiline', False) \
                    .load('/FileStore/tables/drivers.json')



# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema

# COMMAND ----------

my_ddl_schema = """
              Item_Identifier string, 
              Item_Weight string,
              Item_Fat_Content string,
              Item_Visibility double,
              Item_Type string,
              Item_MRP double,
              Outlet_Identifier string,
              Outlet_Establishment_Year integer,
              Outlet_Size string,
              Outlet_Location_Type string,
              Outlet_Type string,
              Item_Outlet_Sales double
              """










# COMMAND ----------

df = spark.read.format('csv')\
              .schema(my_ddl_schema)\    
              .option('header', True)\
              .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([
                    StructField('Item_Visibility',StringType(),True)

])

# COMMAND ----------

df = spark.read.format('csv') \
                    .schema(my_struct_schema)\
                      .option('header',True)\
                        .load('/FileStore/tables/BigMart_Sales.csv/')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT

# COMMAND ----------

df_select = df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###FILTER

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1 : EASY

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

df.filter(col('Item_Fat_Content<10') 'Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2 : MEDIUM

# COMMAND ----------

df.filter((col('Item_Type')== 'Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 3 : HARD

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 3'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WithColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1 : Create a new cloumn based on any exsisting column

# COMMAND ----------

df = df.withColumn('flag',lit('new'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('multipy',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2 - Modify the exsisting column values

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
  .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','LF')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2 : asc()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 3 Advanced : Sorting based on multiple columns 

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 4 - Adv one col - asc and another col - desc

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1 : Drop 1 col

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

df.drop('Item_Visibility','Item_Weight').display()
