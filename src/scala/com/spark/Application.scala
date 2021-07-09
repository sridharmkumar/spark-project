package com.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import java.time._
import java.time.format._
import scala.io.Source

object Application {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-project").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().master("local[*]").enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict").appName("spark-project").getOrCreate()

    import spark.implicits._

    val hive = new HiveContext(sc)
    import hive.implicits._

    println
    println
    println("----- Step 1 : Retrieving Raw Data -----")
    val yesterday = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(1)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val previousDate = formatter format yesterday

    val inputData = spark.read.format("com.databricks.spark.avro").load(s"hdfs:/user/cloudera/data/$previousDate")
    inputData.show()

    println
    println
    println("----- Step 2 : Retrieving API Data -----")
    val apiUrl = "https://randomuser.me/api/0.8/?results=200"
    val apiResponse = Source.fromURL(apiUrl).mkString
    val apiDataFrame = spark.read.json(sc.parallelize(List(apiResponse)))
    apiDataFrame.show()

    println
    println
    println("----- Step 3 : Preparing Flatten Data -----")
    val flattenData = apiDataFrame.withColumn("results", explode(col("results")))
      .select(
        col("nationality"),
        col("results.user.username"),
        col("results.user.cell"),
        col("results.user.dob"),
        col("results.user.email"),
        col("results.user.gender"),
        col("results.user.location.city"),
        col("results.user.location.state"),
        col("results.user.location.street"),
        col("results.user.location.zip"),
        col("results.user.md5"),
        col("results.user.name.first"),
        col("results.user.name.last"),
        col("results.user.name.title"),
        col("results.user.password"),
        col("results.user.phone"),
        col("results.user.picture.large"),
        col("results.user.picture.medium"),
        col("results.user.picture.thumbnail"),
        col("results.user.registered"),
        col("results.user.salt"),
        col("results.user.sha1"),
        col("results.user.sha256")
      )
    flattenData.show()

    println
    println
    println("----- Step 4 : Remove Numerical from Username -----")
    val replacedData = flattenData.withColumn("username", regexp_replace(col("username"), "([0-9])", ""))
    replacedData.show()

    println
    println
    println("----- Step 5 : Joining DataFrames -----")
    val joinData = inputData.join(broadcast(replacedData), Seq("username"), "left")
    joinData.show()

    println
    println
    println("----- Step 6 : Indexing DataFrame -----")
    val indexedData = addColumnIndex(spark, joinData).withColumn("id", col("index")).drop("index")
    indexedData.printSchema()

    println
    println
    println("----- Step 7 : Calculating Max Value -----")
    val maxValueDataFrame = spark.sql("select coalesce(max(id),0) from spark_project.spark_hive_customer")
    val maxValue = maxValueDataFrame.rdd.map(x => x.mkString("")).collect().mkString("").toInt
    println("Max value : " + maxValue)

    println
    println
    println("----- Step 8 : Processing Id Column with Max Value -----")
    val resultDataFrame = indexedData.withColumn("id", col("id") + maxValue)
    resultDataFrame.show()

    println
    println
    println("----- Step 9 : Saving to Hive Table -----")
    resultDataFrame.write.format("hive").mode("append").saveAsTable("spark_project.spark_hive_customer")
    println
    println("Successfully Completed!")
  }


  def addColumnIndex(spark: SparkSession, df: DataFrame) = {
    spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(df.schema.fields :+ StructField("index", LongType, nullable = false)))
  }
}