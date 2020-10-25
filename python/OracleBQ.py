import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import FloatType
from pyspark.sql.functions import date_format, udf, col, avg, stddev, count, desc, rank, round
import sys

class UsedFunctions:

  def println(self,lst):
    for ll in lst:
      print(ll[0])

usedFunctions = UsedFunctions()

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

lst = (sqlContext.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");usedFunctions.println(lst)

# provide Oracle parameters through HDP driver
driverName = "com.ddtek.jdbc.ddhybrid.DDHybridDriver"
HybridServer = "progress"
HybridPort = "8080"
hybridDataPipelineDataSource = "mydb12"
HybridServerUserName = "d2cadmin";
HybridServerPassword = "hduser";
OracleUserName = "scratchpad";
OracleUserPassword = "oracle";
OracleSchema = "SCRATCHPAD"
OracleTable= "weights_date"

sparkAppName = "weights"
spark =  SparkSession.builder. \
    appName(sparkAppName). \
    getOrCreate()
#
## Read Oracle table on prem
#
jdbUrl = "jdbc:datadirect:ddhybrid://"+HybridServer+":"+HybridPort+";hybridDataPipelineDataSource="+ hybridDataPipelineDataSource+";datasourceUserId="+OracleUserName+";datasourcePassword="+OracleUserPassword+";encryptionMethod=noEncryption;"
print("\nThis is JDBC connection used\n"+jdbUrl+"\n")
try:
  OracleDF = spark.read. \
     format("jdbc"). \
     option("url", jdbUrl). \
     option("dbtable", OracleSchema+"."+OracleTable). \
     option("user", HybridServerUserName). \
     option("password", HybridServerPassword). \
     load()
  OracleDF.printSchema()
except Exception as e:
  # Output unexpected Exceptions.
  print(e)
  print(e.__class__.__name__ + ": " + e.message)
  sys.exit(1)

if (len(OracleDF.take(1))) == 0:
  print("\nSource table "+OracleSchema+"."+OracleTable +" is empty. exiting")
  sys.exit(1)

# Back to BigQuery table
tmp_bucket = "tmp_storage_bucket/tmp"

# Set the temporary storage location
spark.conf.set("temporaryGcsBucket",tmp_bucket)
spark.sparkContext.setLogLevel("ERROR")

HadoopConf = sc._jsc.hadoopConfiguration()
HadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
HadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

bq = spark._sc._jvm.com.samelamin.spark.bigquery.BigQuerySQLContext(spark._wrapped._jsqlContext)

#get and set the env variables
projectId ="axial-glow-224522"
datasetLocation="europe-west2"
targetDataset = "test_python"
targetRS = "weights_RS"
targetMD = "weights_MODEL"
targetML = "weights_ML_RESULTS"
outputRS = targetDataset+"."+targetRS
outputMD = targetDataset+"."+targetMD
outputML = targetDataset+"."+targetML
fullyQualifiedoutputRS = projectId+":"+outputRS
fullyQualifiedoutputMD = projectId+":"+outputMD
fullyQualifiedoutputML = projectId+":"+outputML
jsonKeyFile="/home/hduser/GCPFirstProject-d75f1b3a9817.json"

spark.conf.set("GcpJsonKeyFile",jsonKeyFile)
spark.conf.set("BigQueryProjectId",projectId)
spark.conf.set("BigQueryDatasetLocation",datasetLocation)
sqltext = ""
from pyspark.sql.window import Window
wSpec = Window.partitionBy(date_format(col("datetaken"),"EEEE"))
wSpec2 = Window.orderBy(col("weightkg"))
rs1 = OracleDF.filter(col("datetaken").between("2020-01-01","2020-12-31")). \
        select(date_format(col("datetaken"),"EEEE").alias("DOW"), \
        date_format(col("datetaken"),"u").alias("DNUMBER"), \
        avg(col("weight")).over(wSpec).alias("weightkg"), \
        stddev(col("weight")).over(wSpec).alias("StandardDeviation"), \
        count(date_format(col("datetaken"),"EEEE")).over(wSpec).alias("sampleSize")).distinct().orderBy(col("weightkg").desc())

rs2= rs1.select(rank().over(wSpec2).alias("Rank"), \
             col("DOW").alias("DayofWeek"), \
             col("DNUMBER").cast("Integer").alias("DayNumber"), \
             round(col("weightkg"),3).alias("AverageForDayOfWeekInKg"), \
             round(col("StandardDeviation"),2).alias("StandardDeviation"), \
             col("sampleSize").cast("Integer").alias("sampleSizeInDays"))

# Save the result set to a BigQuery table. Table is created if it does not exist
print("\nsaving data to " + fullyQualifiedoutputRS)
rs2. \
    write. \
    format("bigquery"). \
    mode("overwrite"). \
    option("table", fullyQualifiedoutputRS). \
    save()

sqltext1 = "CREATE OR REPLACE MODEL " + outputMD
sqltext2 = """
 OPTIONS
 ( model_type='linear_reg',
   data_split_col='DayNumber',
   data_split_method='seq',
   ls_init_learn_rate=.15,
   l1_reg=1,
   max_iterations=5,
   data_split_eval_fraction=0.3,
   input_label_cols=['AverageForDayOfWeekInKg']
 )
AS SELECT
     DayNUmber
   , AverageForDayOfWeekInKg
   , StandardDeviation
   , sampleSizeInDays
FROM
"""
print("Model "+ outputMD+ " has the following code\n"+sqltext1+sqltext2+outputRS)
print("\nCreating model " + outputMD)
bq.runDMLQuery(sqltext1+sqltext2+outputRS)

print("\n Evaluating results for model " +  outputMD + " with the following code\n" + "CREATE OR REPLACE VIEW "+outputML +"` AS SELECT * FROM ML.EVALUATE(MODEL `"+projectId+"."+outputMD+"`, (SELECT DayNumber,AverageForDayOfWeekInKg,StandardDeviation,sampleSizeInDays FROM `"+ projectId+"."+outputRS + "`))")

sqltext = "CREATE OR REPLACE VIEW "+outputML+" AS SELECT * FROM ML.EVALUATE(MODEL `"+projectId+"."+outputMD+"`, (SELECT DayNumber,AverageForDayOfWeekInKg,StandardDeviation,sampleSizeInDays FROM `"+ projectId+"."+outputRS + "`))"

bq.runDMLQuery(sqltext)

# read data from the view just created
print("\nreading data from "+projectId+":"+outputML +" displaying the ML evaluation parameters")
MLresult = spark.read. \
              format("bigquery"). \
              option("credentialsFile",jsonKeyFile). \
              option("project", projectId). \
              option("parentProject", projectId). \
              option("dataset", targetDataset). \
              option("table", targetML). \
              option("viewsEnabled", "true"). \
              load()

print("\nResults of linear regression fit to Mich's week of day weight measurement averaged over one year\n")
MLresult.select(col("mean_absolute_error").alias("Mean Absolute Error"), \
                col("mean_squared_error").alias("Mean Squared Error"), \
                col("mean_squared_log_error").alias("Mean Squared Log Error"), \
                col("median_absolute_error").alias("Median Absolute Error"), \
                col("r2_score").alias("r2 Score [R squared]"), \
                col("explained_variance").alias("Explained Variance")). \
         show(n=20,truncate=False,vertical=False)

lst = (sqlContext.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");usedFunctions.println(lst)
