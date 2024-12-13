/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import scala.util.Random
import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions_ds._
import org.apache.spark.registrar.DatasketchesFunctionRegistry

class KllTest extends SparkSessionManager {
  import spark.implicits._

  test("KLL Doubles Sketch via scala") {
    val data: Seq[Record] = generateRecords()
    val exactStats = ExactStats()
    data.foreach(exactStats.addRecord)

    val df = spark.createDataset(data)
    assert(df.count() == exactStats.numRecords)

    // Compute aggregate min and max for the column 'uniform'
    val minMaxDF = df.agg(min("uniform").as("min_uniform"), max("uniform").as("max_uniform"))
    val groupDf = df.groupBy($"id")

    // Call kll_sketch_agg outside of agg() and assign it to a temporary variable
    val kllSketchAggExpr = kll_sketch_agg($"gaussian", 200)
    //println(s"kllSketchAggExpr: $kllSketchAggExpr")

    // Use the temporary variable in agg()
    val gaussianKllDf = groupDf.agg(kllSketchAggExpr.as("data"))

    // Optionally, you can extract the values if needed
    val minMax = minMaxDF.collect().head
    val minUniform = minMax.getAs[Double]("min_uniform")
    val maxUniform = minMax.getAs[Double]("max_uniform")

    println(s"Min uniform: $minUniform, Max uniform: $maxUniform")
    gaussianKllDf.show()

    gaussianKllDf.select($"id", kll_get_min($"data").as("min"), kll_get_max($"data").as("max")).orderBy($"id").show()

    //val kllFinalDf = gaussianKllDf.agg(kll_merge_agg($"data").as("merged_kll"))
    //kllFinalDf.select(kll_get_min($"merged_kll").as("min_global"), kll_get_max($"merged_kll").as("max_global")).show()
    gaussianKllDf.select($"id", kll_get_cdf($"data", Array[Double](0.05, 0.5, 0.95)).as("cdf")).orderBy($"id").show()
  }

  test("Kll Doubles Sketch via SQL") {
    // Register Datasketches functions
    DatasketchesFunctionRegistry.registerFunctions(spark)

    val n = 5
    val data = (for (i <- 1 to n) yield i.toDouble).toDF("value")
    data.createOrReplaceTempView("data_table")

    val kllDf = spark.sql(
      s"""
      |SELECT
      |  kll_get_min(kll_sketch_agg(value, 200)) as min_value,
      |  kll_get_max(kll_sketch_agg(value, 200)) as max_value
      |FROM
      |  data_table
    """.stripMargin)
    val minValue = kllDf.collect()(0).getDouble(0)
    val maxValue = kllDf.collect()(0).getDouble(1)
    assert(minValue == 1.0)
    assert(maxValue == 5.0)

    kllDf.show()
  }


  test("KLL Doubles Merge via Scala") {

  }

  test("KLL Doubles Merge via SQL") {

  }

  private def generateData(numRecords: Int = 25000): Seq[Record] = {
    val maxId = 20
    for (i <- 0 until numRecords) yield {
      val id = i % maxId
      Record(id, Random.nextDouble(), id + Random.nextGaussian())
    }
  }
}

case class Record(id: Integer, uniform: Double, gaussian: Double)

case class ExactStats() {
  private val idToUniqVals: mutable.Map[String, mutable.Set[Int]] = mutable.Map.empty
  var numRecords: Long = 0

  def addRecord(record: Record): Unit = {
    numRecords += 1
  }

  def getExactUniqueValues(id: String): Int = idToUniqVals.get(id).map(_.size).getOrElse(0)
}
