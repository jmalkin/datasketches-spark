package org.apache.datasketches.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.SparkConf

trait SparkSessionManager extends AnyFunSuite with BeforeAndAfterAll {
  Logger.getRootLogger().setLevel(Level.INFO)
  //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  //Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)

  val conf = new SparkConf()
    .set("spark.executor.extraJavaOptions", "-Djava.version=11")
    .set("spark.driver.extraJavaOptions", "-Djava.version=11")
    .set("spark.sql.codegen.wholeStage", "true")
    .set("spark.sql.codegen.comments", "true")

  lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("datasketches-spark-tests")
      .master("local[3]")
      .config(conf)
      .getOrCreate()

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("INFO")
  }
}
