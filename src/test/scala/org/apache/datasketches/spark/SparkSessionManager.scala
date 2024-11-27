package org.apache.datasketches.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

trait SparkSessionManager extends AnyFunSuite with BeforeAndAfterAll {
  Logger.getRootLogger().setLevel(Level.OFF)
  //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  //Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)

  lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("datasketches-spark-tests")
      .master("local[3]")
      .getOrCreate()

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
  }
}
