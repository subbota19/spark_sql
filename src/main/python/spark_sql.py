# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL
# MAGIC * Top 10 hotels with max absolute temperature difference by month.
# MAGIC * Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
# MAGIC * For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

# COMMAND ----------

from pyspark import sql
import os

# COMMAND ----------

session = sql.SparkSession.builder.getOrCreate()

# COMMAND ----------

session.sql("create database if not exists spark_sql")
session.sql("use spark_sql")

# COMMAND ----------

session.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(os.getenv("account_load_name")),os.getenv("auth_type"))
session.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(os.getenv("account_load_name")),os.getenv("provider_type"))
session.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(os.getenv("account_load_name")),os.getenv("client_id"))
session.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(os.getenv("account_load_name")),os.getenv("client_secret"))
session.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(os.getenv("account_load_name")),os.getenv("client_endpoint"))
session.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(os.getenv("account_upload_name")),os.getenv("account_key"))

# COMMAND ----------

<<<<<<< HEAD
hotel_weather_DF= session.read.parquet(os.getenv(hotel_weather_path))
expedia_DF = session.read.format("avro").load(os.getenv(expedia_path))
=======
# DBTITLE 1,Creating DFs according to data from Azure storage 
hotel_weather_DF= session.read.parquet(os.getenv("hotel_weather_path"))
expedia_DF = session.read.format("avro").load(os.getenv("expedia_path"))
>>>>>>> developed spark sql hw

# COMMAND ----------

session.sql("drop table if exists hotel_weather")
session.sql("drop table if exists expedia")

# COMMAND ----------

<<<<<<< HEAD
=======
# DBTITLE 1,Creating SQL tables from DFs
>>>>>>> developed spark sql hw
hotel_weather_DF.orderBy("wthr_date").write.saveAsTable("hotel_weather")
expedia_DF.write.saveAsTable("expedia")

# COMMAND ----------

session.sql("optimize hotel_weather")
session.sql("optimize expedia")

# COMMAND ----------

session.sql("drop view if exists expedia_month_sum_view")
session.sql("drop view if exists join_data_view")

# COMMAND ----------

<<<<<<< HEAD
# DBTITLE 1,Support procedures
=======
# MAGIC %md
# MAGIC # Support procedures
# MAGIC * analyze_plan - provide detailed plan information about sql statement
# MAGIC * show_query - execute sql request and demonstrate result
# MAGIC * save_query - execute sql request and save data from table to Azure blo storage

# COMMAND ----------

>>>>>>> developed spark sql hw
def analyze_plan(sql_select:str,is_extended: bool = True) -> None:
  session.sql(sql_select).explain(extended=is_extended)

def show_query(sql_select:str) -> None:
  session.sql(sql_select).show()

<<<<<<< HEAD
def save_query(sql_select:str,table:str,upload_path:str=os.getenv(upload_path),mode:str="overwrite",partition:tuple=()) -> None:
=======
def save_query(sql_select:str,table:str,upload_path:str=os.getenv("upload_path"),mode:str="overwrite",partition:tuple=()) -> None:
>>>>>>> developed spark sql hw
  
  session.sql(sql_select)

  session.table(table).write.parquet(path="{}/{}/".format(upload_path,table),mode=mode,partitionBy=partition)


# COMMAND ----------

session.sql("drop table if exists query_1")

# COMMAND ----------

<<<<<<< HEAD
# DBTITLE 1,Query 1 (calculate difference between months with lag function and order top hotels with max diff)
=======
# MAGIC %md
# MAGIC # Spark SQL - Query 1
# MAGIC * Summarize avg_tmpr_c by id,year,month
# MAGIC * After grouping we need to calculate the difference between previously month and apply abs function

# COMMAND ----------

# MAGIC %md
# MAGIC # Full plan for query 1
# MAGIC 
# MAGIC ** Parsed Logical Plan **
# MAGIC 
# MAGIC *In general on this step we need to pasre our request for checking sql syntax, columns or table name, as we can see: 1) the first entry point feature is CreateTableAsSelectStatement that allows to create table as select in case of need; 2) define limit clause in query (in our case is 10); 3) define sort type and values (in our case is sum_avg_tmpr_c_diff with DESC); 4) in Project feature we can select number of select statements (names, definitions); 5) drafting subqueries are for selecting data from a table referenced in the outer query; 6) Add aggregate functions (in our case sum for avg_tmpr_c ). *
# MAGIC 
# MAGIC ** Analyzed Plan **
# MAGIC 
# MAGIC *After parsing logical plan we can start to analyze (resolve semantics) it, for this, we will use catalog (repository with info about Spark table, DF or dataset): 1) num_affected_rows function returns the number of affected rows 2) feature CreateTableAsSelectStatement allows to create table according to DeltaCatalog; 3) define limit clause in query (in our case is 10); 4) define sort type and values (in our case is sum_avg_tmpr_c_diff with DESC); 5) analytics fucntions in context; 6) in Project feature we can select number of select statements (names, definitions); 7) drafting subqueries are for selecting data from a table referenced in the outer query; 8) Add aggregate functions (in our case sum for avg_tmpr_c ); 9) setting up relations from Catalog. *
# MAGIC 
# MAGIC ** Optimized Logical Plan **
# MAGIC 
# MAGIC *In this step, we can start to optimize the plan using inner functionality - Catalust Optimize (CO) : in general, all steps from previous plan are the same, but in our case, CO will try to checks all the tasks which can be performed and computed together in one stage and optimize the query by evaluating the filter clause. *
# MAGIC 
# MAGIC ** Physical Plan **
# MAGIC 
# MAGIC *This important plan generates different kinds of execution strategies and then keeps comparing them in the Cost Model (it estimates the execution time and resources to be taken by each strategy). Finally, whichever strategy is going to be the best optimal one is selected as the Best Physical Plan : 1) the entry point feature is AtomicCreateTableAsSelect for creating table and checking num_affected_rows and num_inserted_rows; 2) involve feature TakeOrderedAndProject that allows giving a sub-query block a name , which can be referenced in several places within the query; 3) define sort type and values (in our case is sum_avg_tmpr_c_diff with DESC); 4) apply Exchange hashpartitioning for defining which rows are distributed across partitions based on the MurMur3 hash of partitioning expressions, because in our case we will group values (with default total count of partitions = 200); 5) apply HashAggregate operator, because in our case we will use sum function; 6) apply FileScan (it's logical scans over data sources) for batch queries. *

# COMMAND ----------

>>>>>>> developed spark sql hw
query=""" 
          create table if not exists query_1 as
          
          select id,year,month,abs(sum_avg_tmpr_c - lag(sum_avg_tmpr_c) over (partition by id,year order by month)) as sum_avg_tmpr_c_diff from 
          
          (select id,year,month,sum(avg_tmpr_c) as sum_avg_tmpr_c from hotel_weather group by id,year,month)
          
          order by sum_avg_tmpr_c_diff desc limit 10"""

analyze_plan(query)
<<<<<<< HEAD
save_query(query)

# COMMAND ----------

session.sql("drop view if exists expedia_month_sum_view")

# COMMAND ----------

# DBTITLE 1,Query 2 ( we need to create a view with a pivot that allows on the next step select hotels by months )
session.sql("""

        create view expedia_month_sum_view as 
        
        select 
        
        hotel_id,
        coalesce(sum(Jan),0) as sum_Jan,
        coalesce(sum(Feb),0) as sum_Feb,
        coalesce(sum(Mar),0) as sum_Mar,
        coalesce(sum(Apr),0) as sum_Apr,
        coalesce(sum(May),0) as sum_May,
        coalesce(sum(Jun),0) as sum_Jun,
        coalesce(sum(Jul),0) as sum_Jul,
        coalesce(sum(Aug),0) as sum_Aug,
        coalesce(sum(Sep),0) as sum_Sep,
        coalesce(sum(Oct),0) as sum_Oct,
        coalesce(sum(Nov),0) as sum_Nov,
        coalesce(sum(Dec),0) as sum_Dec 
        
        from (
        
        select hotel_id,month(srch_ci) as srch_ci_month,month(srch_co) as srch_co_month from expedia) 

        pivot (count(*) for srch_ci_month in (1 as Jan,2 as Feb,3 as Mar,4 as Apr,5 as May,6 as Jun,7 as Jul,8 as Aug,9 as Sep,10 as Oct,11 as Nov,12 as Dec))
        
        group by hotel_id
=======

# COMMAND ----------

session.sql("drop view if exists expedia_month_view")

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark SQL - Query 2
# MAGIC * First, we need to create a view that allows calculating the order expedia count for each month and hotel according to srch_co and srch_ci using maxtrix crossing and case functionality (as example order with date from 31.12.2018 to 02.01.2019 will refer to Jan (+1) and Dec (+1))
# MAGIC * Select top 10 hotels for each month according to the max count by grouping and filtering 

# COMMAND ----------

month_touple = "(1,2,3,4,5,6,7,8,9,10,11,12)"

session.sql(f"""
        
        create view expedia_month_view as 
        
        select hotel_id,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 1) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 1) then 1 end) as jan,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 2) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 2) then 1 end) as feb,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 3) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 3) then 1 end) as mar,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 4) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 4) then 1 end) as apr,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 5) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 5) then 1 end) as may,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 6) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 6) then 1 end) as jun,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 7) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 7) then 1 end) as jul,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 8) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 8) then 1 end) as aug,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 9) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 9) then 1 end) as sep,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 10) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 10) then 1 end) as oct,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 11) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 11) then 1 end) as nov,
        
        count(case when (month(srch_ci) in {month_touple} and month(srch_co) = 12) or
        (month(srch_co) in {month_touple} and month(srch_ci) = 12) then 1 end) as dec
        
        from (select hotel_id,srch_ci,srch_co from expedia)
        
        group by hotel_id
        
>>>>>>> developed spark sql hw
        """)

# COMMAND ----------

<<<<<<< HEAD
# DBTITLE 1,Iterating through months for selecting hotels with a view
for month in ['sum_Jan','sum_Feb','sum_Mar','sum_Apr','sum_May','sum_Jun','sum_Jul','sum_Aug','sum_Sep','sum_Oct','sum_Nov','sum_Dec']:
    session.sql(f"drop table if exists query_2_{month}")
    query=f"create table if not exists query_2_{month} as select hotel_id,{month} from expedia_month_sum_view order by {month} desc limit 10"
    analyze_plan(query)
    save_query(query,"query_2_{}".format(month))
=======
# DBTITLE 1,Iterating through months for selecting top 10 hotels according to the view
for month in ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']:
    session.sql(f"drop table if exists query_2_{month}")
    query = f"create table if not exists query_2_{month} as select hotel_id,{month} from expedia_month_view order by {month} desc limit 10"
    analyze_plan(query)
    save_query(query,f"query_2_{month}")
>>>>>>> developed spark sql hw

# COMMAND ----------

session.sql("drop table if exists query_3")
session.sql("drop view if exists join_data_view")

# COMMAND ----------

<<<<<<< HEAD
# DBTITLE 1,Query 3 ( we need to create a view as joining tables with selecting and ordering by id and weather date )
=======
# MAGIC %md
# MAGIC # Spark SQL - Query 3
# MAGIC * First, we need to create a view by joining expedia and hotel_weather, sort data by condition - extended stay (more than 7 days) and logical restriction - available weather in this period
# MAGIC * Select expedia orders with calculating average, the temperature difference between first & last days (important note: for order without weather info in first or last days difference is null)

# COMMAND ----------

# DBTITLE 0,Query 3 ( we need to create a view as joining tables with selecting and ordering by id and weather date )
>>>>>>> developed spark sql hw
session.sql("""
          create view join_data_view as
          
          select hw.id,hw.avg_tmpr_c,hw.wthr_date,ex.srch_ci,ex.srch_co
          
          from hotel_weather as hw inner join expedia as ex on hw.id = ex.hotel_id 
          where hw.wthr_date>=ex.srch_ci and hw.wthr_date<=ex.srch_co and datediff(ex.srch_co,ex.srch_ci)>7
         
          order by hw.id,hw.wthr_date
          """)

# COMMAND ----------

# DBTITLE 1,Calculating average, first & last temperature difference for each order
query = """
          create table if not exists query_3 as
          
          select id,avg(avg_tmpr_c) as avg_order_tmpr_c,srch_ci,srch_co,
          
          case when max(wthr_date)=srch_co and min(wthr_date)=srch_ci 
          then last(avg_tmpr_c)-first(avg_tmpr_c) else null end as tmpr_diff
          
          from join_data_view
          
          group by id,srch_ci,srch_co 
          
          """
analyze_plan(query)
save_query(query,"query_3",partition=("id"))
