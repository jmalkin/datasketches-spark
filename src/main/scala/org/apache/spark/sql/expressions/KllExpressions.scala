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

package org.apache.spark.sql.expressions

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria
import org.apache.spark.sql.types.KllDoublesSketchType

import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BooleanType, DataType, DoubleType}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpectsInputTypes, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.{UnaryExpression, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.{Literal, NullIntolerant, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeBlock, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.trees.TernaryLike

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the minimum value seem by the sketch given the binary representation
    of a Datasketches KllDoublesSketch. """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col)) FROM VALUES (1.0), (2.0), (3.0) tab(col);
       1.0
  """
  //group = "misc_funcs",
)
case class KllGetMin(child: Expression)
 extends UnaryExpression
 with ExpectsInputTypes
 with NullIntolerant {

  override protected def withNewChildInternal(newChild: Expression): KllGetMin = {
    copy(child = newChild)
  }

  override def prettyName: String = "kll_get_min"

  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(input: Any): Any = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val sketch = KllDoublesSketch.wrap(Memory.wrap(bytes))
    sketch.getMinItem
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    val childEval = child.genCode(ctx)
    val sketch = ctx.freshName("sketch")

    val code =
      s"""
         |${childEval.code}
         |final org.apache.datasketches.kll.KllDoublesSketch $sketch = org.apache.spark.sql.types.KllDoublesSketchType.wrap(${childEval.value});
         |final double ${ev.value} = $sketch.getMinItem();
       """.stripMargin
    ev.copy(code = CodeBlock(Seq(code), Seq.empty), isNull = childEval.isNull)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the maximum value seem by the sketch given the binary representation
    of a Datasketches KllDoublesSketch. """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col)) FROM VALUES (1.0), (2.0), (3.0) tab(col);
       3.0
  """
  //group = "misc_funcs",
)
case class KllGetMax(child: Expression)
 extends UnaryExpression
 with ExpectsInputTypes
 with NullIntolerant {

  override protected def withNewChildInternal(newChild: Expression): KllGetMax = {
    copy(child = newChild)
  }

  override def prettyName: String = "kll_get_max"

  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(input: Any): Any = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val sketch = KllDoublesSketch.wrap(Memory.wrap(bytes))
    sketch.getMaxItem
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    val childEval = child.genCode(ctx)
    val sketch = ctx.freshName("sketch")

    val code =
      s"""
         |${childEval.code}
         |final org.apache.datasketches.kll.KllDoublesSketch $sketch = org.apache.spark.sql.types.KllDoublesSketchType.wrap(${childEval.value});
         |final double ${ev.value} = $sketch.getMaxItem();
       """.stripMargin
    ev.copy(code = CodeBlock(Seq(code), Seq.empty), isNull = childEval.isNull)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr, expr, isInclusive) - Returns an approximation to the PMF
      of the given KllDoublesSketch using the specified search criteria (default: inclusive, isInclusive = true)
      or exclusive using the given split points.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col), array(1.5, 3.5)) FROM VALUES (1.0), (2.0), (3.0) tab(col);
       [0.3333333333333333, 0.6666666666666666, 0.0]
  """
)
case class KllGetPmf(first: Expression,
                     second: Expression,
                     third: Expression)
    extends RuntimeReplaceable
    with ImplicitCastInputTypes
    with TernaryLike[Expression] {

    def this(first: Expression, second: Expression) = {
        this(first, second, Literal(true))
    }

    override lazy val replacement: Expression = KllGetPmfCdf(first, second, third, true)
    override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType, ArrayType(DoubleType), BooleanType)
    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
        copy(first = newFirst, second = newSecond, third = newThird)
    }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr, expr, isInclusive) - Returns an approximation to the PMF
      of the given KllDoublesSketch using the specified search criteria (default: inclusive, isInclusive = true)
      or exclusive using the given split points.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col), array(1.5, 3.5)) FROM VALUES (1.0), (2.0), (3.0) tab(col);
       [0.3333333333333333, 0.6666666666666666, 0.0]
  """
)
case class KllGetCdf(first: Expression,
                     second: Expression,
                     third: Expression)
    extends RuntimeReplaceable
    with ImplicitCastInputTypes
    with TernaryLike[Expression] {

    def this(first: Expression, second: Expression) = {
        this(first, second, Literal(true))
    }

    override lazy val replacement: Expression = KllGetPmfCdf(first, second, third, false)
    override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType, ArrayType(DoubleType), BooleanType)
    override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
        copy(first = newFirst, second = newSecond, third = newThird)
    }
}


/**
  * Returns the PMF and CDF of the given quantile search criteria.
  *
  * @param first A KllDoublesSketch sketch, in serialized form
  * @param second An array of split points, as doubles
  * @param third A boolean flag for inclusive mode. If true, use INCLUSIVE else EXCLUSIVE
  * @param isPmf Whether to return the PMF (true) or CDF (false)
  */
@ExpressionDescription(
  usage = """
    _FUNC_(expr, expr, isInclusive, isPmf) - Returns an approximation to the PMF or CDF (default: isPmf = false)
      of the given KllDoublesSketch using the specified search criteria (default: inclusive, isInclusive = true)
      or exclusive using the given split points.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg(col), array(1.5, 3.5), true, true) FROM VALUES (1.0), (2.0), (3.0) tab(col);
       [0.3333333333333333, 0.6666666666666666, 0.0]
  """
)
case class KllGetPmfCdf(first: Expression,
                        second: Expression,
                        third: Expression,
                        isPmf: Boolean = false)
 extends TernaryExpression
 with ExpectsInputTypes
 with NullIntolerant
 with ImplicitCastInputTypes {

  lazy val isInclusive = third.eval().asInstanceOf[Boolean]

  override protected def withNewChildrenInternal(newFirst: Expression,
                                                 newSecond: Expression,
                                                 newThird: Expression) = {
    copy(first = newFirst, second = newSecond, third = newThird, isPmf = isPmf)
  }

  override def prettyName: String = "kll_get_pmf_cdf"

  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType, ArrayType(DoubleType), BooleanType)

  override def dataType: DataType = ArrayType(DoubleType, containsNull = false)

  override def nullSafeEval(firstInput: Any, secondInput: Any, thirdInput: Any): Any = {
    val sketchBytes = firstInput.asInstanceOf[Array[Byte]]
    val splitPoints = secondInput.asInstanceOf[GenericArrayData].toDoubleArray
    val sketch = KllDoublesSketch.wrap(Memory.wrap(sketchBytes))

    val result: Array[Double] =
      if (isPmf) {
        sketch.getPMF(splitPoints, if (isInclusive) QuantileSearchCriteria.INCLUSIVE else QuantileSearchCriteria.EXCLUSIVE)
      } else {
        sketch.getCDF(splitPoints, if (isInclusive) QuantileSearchCriteria.INCLUSIVE else QuantileSearchCriteria.EXCLUSIVE)
      }
    new GenericArrayData(result)
  }

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: (String, String, String) => String): ExprCode = {
    val sketchEval = first.genCode(ctx)
    val sketch = ctx.freshName("sketch")
    val splitPointsEval = second.genCode(ctx)
    val code =
      s"""
         |${sketchEval.code}
         |${splitPointsEval.code}
         |if (!${sketchEval.isNull} && !${splitPointsEval.isNull}) {
         |  org.apache.datasketches.quantilescommon.QuantileSearchCriteria searchCriteria = ${if (isInclusive) "org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE" else "org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE"};
         |  final org.apache.datasketches.kll.KllDoublesSketch $sketch = org.apache.spark.sql.types.KllDoublesSketchType.wrap(${sketchEval.value});
         |  final double[] splitPoints = ((org.apache.spark.sql.catalyst.util.GenericArrayData)${splitPointsEval.value}).toDoubleArray();
         |  final double[] result = ${if (isPmf) s"$sketch.getPMF(splitPoints, searchCriteria)" else s"$sketch.getCDF(splitPoints, searchCriteria)"};
         |  org.apache.spark.sql.catalyst.util.GenericArrayData ${ev.value} = new org.apache.spark.sql.catalyst.util.GenericArrayData(result);
         |}
       """.stripMargin
    ev.copy(code = CodeBlock(Seq(code), Seq.empty))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (arg1, arg2, arg3) => s"($arg1, $arg2, $arg3)")
  }
}

// default search criteria = inclusive
// getQuantile(rank, QuantileSearchCriteria)
// getQuantileLowerBound(rank)
// getQuantileUpperBound(rank)
// getQuantiles(double ranks[], QuantileSearchCriteria)
// getRank(quantile, QuantileSearchCriteria)
// getRanks(quantile[]), QuantileSearchCriteria)
// getNormalizedRankError(bool isPmf)
// isEstimationMode()
// toString(bool, bool) -- already part of the wrapper
// getK() ?
// getNumRetained() ?
