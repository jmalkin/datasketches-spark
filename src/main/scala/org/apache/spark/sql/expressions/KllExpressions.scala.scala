package org.apache.spark.sql.expressions

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpectsInputTypes, UnaryExpression}
//import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, KllDoublesSketchType}
//import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

case class KllGetMin(child: Expression)
 extends UnaryExpression
 with CodegenFallback // TODO: investigate if codegen provides any benefit
 with ExpectsInputTypes
 with NullIntolerant {

  override protected def withNewChildInternal(newChild: Expression): KllGetMin = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(input: Any): Any = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val sketch = KllDoublesSketch.wrap(Memory.wrap(bytes))
    sketch.getMinItem
  }
}

case class KllGetMax(child: Expression)
 extends UnaryExpression
 with CodegenFallback // TODO: investigate if codegen provides any benefit
 with ExpectsInputTypes
 with NullIntolerant {

  override protected def withNewChildInternal(newChild: Expression): KllGetMax = {
    copy(child = newChild)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(KllDoublesSketchType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(input: Any): Any = {
    val bytes = input.asInstanceOf[Array[Byte]]
    val sketch = KllDoublesSketch.wrap(Memory.wrap(bytes))
    sketch.getMaxItem
  }
}
