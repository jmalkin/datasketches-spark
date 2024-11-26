package org.apache.datasketches.spark

import scala.util.Random
import scala.collection.mutable

class ThetaTest extends BaseTest {
  test("theta") {
    println("TEST!!!!")
    val data: Seq[Record] = generateRecords()
    val exactStats = ExactStats()
    data.foreach(exactStats.addRecord)

    val df = spark.createDataFrame(data)
    assert(df.count() == exactStats.numRecords)

    // create a theta sketch

    // val df = spark.read.json("src/test/resources/test.json")
    // val sketch = df.thetaSketch("value")
    // val result = sketch.query(QuickSelectSketch.builder().build())
    // assert(result.getEstimate === 1000)
/*
    val sketch = records
      .groupBy($"id")
      .agg(theta_sketch_build($"value").as("distinct_values_sketch"))

 // ---- Build Sketch
    val distinctUserIdsTheta = users
      .groupBy($"name")
      .agg(theta_sketch_build($"id").as("distinct_ids_sketch"))

    val schemaFields = distinctUserIdsTheta.schema.fields
    assert(schemaFields.length == 2)

    val thetaSketchField: StructField = distinctUserIdsTheta.schema.fields(1)
    assert(thetaSketchField.dataType == ThetaSketchType)

    // ---- Evaluate Sketch
    val evaluatedValues = distinctUserIdsTheta
      .select($"name", theta_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))

    val approxDistinctUserIdsPairs = evaluatedValues.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
*/

  }


  private def generateRecords(numRecords: Int = 25000, maxValue: Int = 5000): Seq[Record] = {
    val maxId = 20
    for (i <- 0 until numRecords) yield {
      Record((i % maxId).toString, Random.nextInt(100), Random.nextInt(maxValue))
    }
  }

  case class Record(id: String, rand: Int, value: Int)

  // want to be able to count exact unique values per id
  case class ExactStats() {
    private val idToUniqVals: mutable.Map[String, mutable.Set[Int]] = mutable.Map.empty
    var numRecords: Long = 0

    def addRecord(record: Record): Unit = {
      idToUniqVals.getOrElseUpdate(record.id, new mutable.HashSet[Int]()).add(record.value)
      numRecords += 1
    }

    def getExactUniqueValues(id: String): Int = idToUniqVals.get(id).map(_.size).getOrElse(0)
  }
}