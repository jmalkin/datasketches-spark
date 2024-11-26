package org.apache.datasketches.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

abstract class BaseTest extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder
    .appName("datasketches-spark-tests")
    .master("local[2]")
    .getOrCreate()

  override protected def beforeAll(): Unit = {
    ; // do nothing for now
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}
