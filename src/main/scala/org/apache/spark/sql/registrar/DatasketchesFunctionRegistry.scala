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

package org.apache.spark.registrar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, FunctionRegistryBase}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

import scala.reflect.ClassTag

// DataSketches imports
import org.apache.spark.sql.aggregate.{KllDoublesSketchAgg, KllDoublesMergeAgg}


// based on org.apache.spark.sql.catalyst.FunctionRegistry
trait DatasketchesFunctionRegistry {
  // override this to define the actual functions
  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)]

  //def registerFunction(name: String, info: ExpressionInfo, builder: FunctionBuilder): Unit = {
  //  expressions += (name -> (info, builder))
  //}

  // registers all the functions in the expressions Map
  def registerFunctions(spark: SparkSession): Unit = {
    expressions.foreach { case (name, (info, builder)) =>
      spark.sessionState.functionRegistry.registerFunction(FunctionIdentifier(name), info, builder)
    }
  }

  // simplifies defining the expression (ignoring "since" as a stand-alone library)
  protected def expression[T <: Expression : ClassTag](name: String): (String, (ExpressionInfo, FunctionBuilder)) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name, None)
    (name, (expressionInfo, builder))
  }

}

// defines the Map for the Datasketches functions
object DatasketchesFunctionRegistry extends DatasketchesFunctionRegistry {
  override val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    // Define your function entries here
    // "functionName" -> (expressionInfo, functionBuilder)
    expression[KllDoublesSketchAgg]("kll_sketch_agg"),
    expression[KllDoublesMergeAgg]("kll_merge")
  )
}