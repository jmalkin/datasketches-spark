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

package org.apache.spark.sql.aggregate

//import org.apache.datasketches.common.SketchesArgumentException

//import org.apache.datasketches.memory.{Memory, WritableMemory}
//import org.apache.datasketches.common.Family
import org.apache.datasketches.kll.{KllSketch, KllDoublesSketch}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
//import org.apache.spark.sql.catalyst.expressions.{ImplicitCastInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, LongType, NumericType, FloatType, DoubleType}

//import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.KllDoublesSketchType

// TODO: write a useful javadoc, including Example in the description portion
/**
 * The KllDoublesSketchAgg function utilizes a Datasketches KllDoublesSketch instance to ...
 *
 * See [[https://datasketches.apache.org/docs/HLL/HLL.html]] for more information.
 *
 * @param child child expression against which unique counting will occur
 * @param k the size-accraucy trade-off parameter for the sketch
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, k) - Returns the KllDoublesSketch's binary representation.
      `k` (optional) the size-accuracy trade-off parameter.""",
  examples = """
    Examples:
      > SELECT theta_sketch_estimate(_FUNC_(col, 12)) FROM VALUES (1), (1), (2), (2), (3) tab(col);
       3
  """,
  //group = "agg_funcs",
)
// scalastyle:on line.size.limit
case class KllDoublesSketchAgg(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    constructorFlag: String = "")
  extends TypedImperativeAggregate[KllDoublesSketch]
    with BinaryLike[Expression]
    with ExpectsInputTypes {

  println(s"Primary constructor called with flag: $constructorFlag")
  println("Value of right: " + right)
  println("Value of left: " + left)

  lazy val k: Int = {
    right.eval() match {
      case null => KllSketch.DEFAULT_K
      case k: Int => k
      case _ => throw new SparkUnsupportedOperationException(
        s"Unsupported input type ${right.dataType.catalogString}",
        Map("dataType" -> dataType.toString))
    }
  }

  // Constructors

  def this(child: Expression) = {
    this(child, Literal(KllSketch.DEFAULT_K), 0, 0, "Constructor with child: Expression")
  }

  def this(child: Expression, k: Expression) = {
    this(child, k, 0, 0, "Constructor with child: Expression, k: Expression")
  }

  def this(child: Expression, k: Int) = {
    this(child, Literal(k), 0, 0, "Constructor with child: Expression, k: Int")
  }

  // Copy constructors

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): KllDoublesSketchAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): KllDoublesSketchAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newLeft: Expression,
                                                 newRight: Expression): KllDoublesSketchAgg = {
    println(s"withNewChildrenInternal called with newLeft: $newLeft, newRight: $newRight")
    val newAgg = copy(left = newLeft, right = newRight)
    println(s"New KllDoublesSketchAgg instance created: $newAgg")
    newAgg
  }

  // overrides for TypedImperativeAggregate
  override def prettyName: String = "kll_sketch_agg"

  override def dataType: DataType = KllDoublesSketchType

  override def nullable: Boolean = false

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType, LongType, FloatType, DoubleType)

  // create buffer
  override def createAggregationBuffer(): KllDoublesSketch = KllDoublesSketch.newHeapInstance(k)

  // update
  override def update(sketch: KllDoublesSketch, input: InternalRow): KllDoublesSketch = {
    val value = left.eval(input)
    if (value != null) {
      left.dataType match {
        case DoubleType => sketch.update(value.asInstanceOf[Double])
        case FloatType => sketch.update(value.asInstanceOf[Float].toDouble)
        case IntegerType => sketch.update(value.asInstanceOf[Int].toDouble)
        case LongType => sketch.update(value.asInstanceOf[Long].toDouble)
        case _ => throw new SparkUnsupportedOperationException(
          s"Unsupported input type ${left.dataType.catalogString}",
          Map("dataType" -> dataType.toString))
      }
    }
    sketch
  }

  // union (merge)
  override def merge(sketch: KllDoublesSketch, other: KllDoublesSketch): KllDoublesSketch = {
    if (other != null && !other.isEmpty) {
      sketch.merge(other)
    }
    sketch
  }

  // eval
  override def eval(sketch: KllDoublesSketch): Any = {
    if (sketch == null || sketch.isEmpty) {
      null
    } else {
      sketch.toByteArray
    }
  }

  override def serialize(sketch: KllDoublesSketch): Array[Byte] = {
    KllDoublesSketchType.serialize(sketch)
  }

  override def deserialize(bytes: Array[Byte]): KllDoublesSketch = {
    KllDoublesSketchType.deserialize(bytes)
  }
}
