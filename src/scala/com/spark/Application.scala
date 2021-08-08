package com.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

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
    println("----- Step 1 : Retrieving AVRO Data -----")
    val yesterday = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(1)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val yesterdayDate = formatter format yesterday

    println("Yesterday's Date : " + yesterdayDate)

    val avroData = spark.read.format("com.databricks.spark.avro").load(s"hdfs:/user/cloudera/data/$yesterdayDate")
    println("AVRO Schema:")
    avroData.printSchema()

    println("AVRO Data:")
    avroData.show(5)

    println
    println("----- Step 2 : Retrieving WebAPI Data -----")
    val apiUrl = "https://randomuser.me/api/0.8/?results=200"
    val apiResponse = Source.fromURL(apiUrl).mkString
    val webData = spark.read.json(sc.parallelize(List(apiResponse)))

    println("Web Data Schema:")
    webData.printSchema()

    println("Web Data:")
    webData.show(5)

    println
    println("----- Step 3 : Flattening WebAPI Data -----")
    val flattenData = webData.withColumn("results", explode(col("results")))
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

    println("Flatten Web Data Schema:")
    webData.printSchema()

    println("Flatten Web Data:")
    flattenData.show(5)

    println
    println("----- Step 4 : Removing numerical from username column -----")
    val processedData = flattenData.withColumn("username", regexp_replace(col("username"), "([0-9])", ""))
    processedData.show(5)

    println
    println("----- Step 5 : Joining AVRO & WebAPI Data -----")
    val consolidatedData = avroData.join(broadcast(processedData), Seq("username"), "left")
    consolidatedData.show(5)

    println
    println("----- Step 6 : Adding Index Column (ZipWithIndex) -----")
    val indexedData = addColumnIndex(spark, consolidatedData).withColumn("id", col("index")).drop("index")
    indexedData.printSchema()

    println
    println("----- Step 7 : Retrieving Max Value from Hive table -----")
    val maxValueDataFrame = spark.sql("select coalesce(max(id),0) from spark_project.spark_hive_customer")
    val maxValue = maxValueDataFrame.rdd.map(x => x.mkString("")).collect().mkString("").toInt
    println("Max Value : " + maxValue)

    println
    println("----- Step 8 : Adding Max Value to ID Column -----")
    val finalData = indexedData.withColumn("id", col("id") + maxValue)
    finalData.show()

    println
    println("----- Step 9 : Saving to Hive Table -----")
    finalData.write.format("hive").mode("append").saveAsTable("spark_project.spark_hive_customer")
    println
    println("Final Result-set Successfully Written to Hive!!!")
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