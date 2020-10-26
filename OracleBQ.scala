import java.sql.SQLException;
import scala.util.{Try, Success, Failure}

import org.apache.spark.sql.SparkSession
import com.samelamin.spark.bigquery._

val driverName = "com.ddtek.jdbc.ddhybrid.DDHybridDriver"
val  HybridServer = "progress"
val  HybridPort = "8080"
val  hybridDataPipelineDataSource = "mydb12"
val  HybridServerUserName = "d2cadmin"
val  HybridServerPassword = "hduser"
val  OracleUserName = "scratchpad"
val  OracleUserPassword = "oracle"
val  OracleTable= "weights_date"
val  OracleSchema = OracleUserName
var  e:SQLException = null

val sparkAppName = "weights"
val spark =  SparkSession.
    builder().
    appName(sparkAppName).
    getOrCreate()

println ("\nStarted at"); spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ").collect.foreach(println)
// Build the JDBC connection
val jdbUrl = "jdbc:datadirect:ddhybrid://"+HybridServer+":"+HybridPort+";hybridDataPipelineDataSource="+ hybridDataPipelineDataSource+";datasourceUserId="+OracleUserName+";datasourcePassword="+OracleUserPassword+";encryptionMethod=noEncryption;"
    println("\nThis is JDBC connection used\n"+jdbUrl+"\n")

// Read Oracle table on prem
val OracleDF = Try(
     spark.read.
     format("jdbc").
     option("url", jdbUrl).
     option("dbtable", OracleSchema+"."+OracleTable).
     option("user", HybridServerUserName).
     option("password", HybridServerPassword).
     load()
    ) match {
                   case Success(df) => df
                   case Failure(e) => 
                     println(e)
                     sys.exit(1)
     }

if (OracleDF.take(1).isEmpty){
  println("\nSource table "+OracleSchema+"."+OracleTable +" is empty. exiting")
  sys.exit(1)
}

// Back to BigQuery table
val tmp_bucket = "tmp_storage_bucket/tmp"

// Set the temporary storage location
spark.conf.set("temporaryGcsBucket",tmp_bucket)

import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")

val HadoopConf = spark.sparkContext.hadoopConfiguration
//get and set the env variables
HadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
HadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

val projectId ="axial-glow-224522"
val targetDataset = "test"
val targetRS = "weights_RS"
val targetMD = "weights_MODEL"
val targetML = "weights_ML_RESULTS"
val outputRS = targetDataset+"."+targetRS
val outputMD = targetDataset+"."+targetMD
val outputML = targetDataset+"."+targetML
val fullyQualifiedoutputRS = projectId+":"+outputRS
val fullyQualifiedoutputMD = projectId+":"+outputMD
val fullyQualifiedoutputML = projectId+":"+outputML
val jsonKeyFile="/home/hduser/GCPFirstProject-d75f1b3a9817.json"
val datasetLocation="europe-west2"
spark.conf.set("GcpJsonKeyFile",jsonKeyFile)
spark.conf.set("BigQueryProjectId",projectId)
spark.conf.set("BigQueryDatasetLocation",datasetLocation)
var sqltext = ""
OracleDF.printSchema
import org.apache.spark.sql.expressions.Window
val wSpec = Window.partitionBy(date_format('datetaken,"EEEE"))
val wSpec2 = Window.orderBy('weightkg)
val rs1 = OracleDF.filter('datetaken.between("2020-01-01","2020-12-31")).
        select(date_format('datetaken,"EEEE").as("DOW"),
        date_format('datetaken,"u").as("DNUMBER"),
        avg('weight).over(wSpec).as("weightkg"),
        stddev('weight).over(wSpec).as("StandardDeviation"),
        count(date_format('datetaken,"EEEE")).over(wSpec).as("sampleSize")).distinct.orderBy(desc("weightkg"))

val rs2= rs1.select(rank.over(wSpec2).as("Rank"),
             'DOW.as("DayofWeek"),
             'DNUMBER.cast("Integer").as("DayNumber"),
             round('weightkg,3).as("AverageForDayOfWeekInKg"),
             round('StandardDeviation,2).as("StandardDeviation"),
             'sampleSize.cast("Integer").as("sampleSizeInDays"))

// Save the result set to a BigQuery table. Table is created if it does not exist
println("\nsaving data to " + fullyQualifiedoutputRS)
rs2.
    write.
    format("bigquery").
    mode(org.apache.spark.sql.SaveMode.Overwrite).
    option("table", fullyQualifiedoutputRS).
    save()

var sqltext1 = "CREATE OR REPLACE MODEL " + outputMD
var sqltext2 =
"""
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
println("Model "+ outputMD+ " has the following code\n"+sqltext1+sqltext2+outputRS)
println("\nCreating model " + outputMD)
spark.sqlContext.runDMLQuery(sqltext1+sqltext2+outputRS)
println("\n Evaluating results for model " +  outputMD + " with the following code\n" + "CREATE OR REPLACE VIEW "+outputML +"` AS SELECT * FROM ML.EVALUATE(MODEL `"+projectId+"."+outputMD+"`, (SELECT DayNumber,AverageForDayOfWeekInKg,StandardDeviation,sampleSizeInDays FROM `"+ projectId+"."+outputRS + "`))")

sqltext = "CREATE OR REPLACE VIEW "+outputML+" AS SELECT * FROM ML.EVALUATE(MODEL `"+projectId+"."+outputMD+"`, (SELECT DayNumber,AverageForDayOfWeekInKg,StandardDeviation,sampleSizeInDays FROM `"+ projectId+"."+outputRS + "`))"

spark.sqlContext.runDMLQuery(sqltext)

//read data from the view just created
println("\nReading data from View "+outputML+" displaying the ML evaluation parameters")
//val MLresult = spark.sqlContext.bigQueryTable(fullyQualifiedoutputML)
val MLresult = spark.read.
              format("bigquery").
              option("credentialsFile",jsonKeyFile).
              option("project", projectId).
              option("parentProject", projectId).
              option("dataset", targetDataset).
              option("table", targetML).
              option("viewsEnabled", "true").
              load()

println("\nResults of linear regression fit to Mich's week of day weight measurement averaged over one year\n")
MLresult.select('mean_absolute_error.as("Mean Absolute Error"),
                'mean_squared_error.as("Mean Squared Error"),
                'mean_squared_log_error.as("Mean Squared Log Error"),
                'median_absolute_error.as("Median Absolute Error"),
                'r2_score.as("r2 Score [R squared]"),
                'explained_variance.as("Explained Variance")).
         show(false)
println ("\nFinished at"); spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ").collect.foreach(println)
sys.exit()
