package org.apache.spark.sql.expressions

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpectsInputTypes, UnaryExpression}
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, KllDoublesSketchType}
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeBlock, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback

case class KllGetMin(child: Expression)
 extends UnaryExpression
 with CodegenFallback
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

  /*
  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    val bytes = ctx.freshName("bytes")
    val sketch = ctx.freshName("sketch")
    val code =
      s"""
         |final byte[] $bytes = ${f(child.genCode(ctx).value)}.getBytes();
         |final org.apache.datasketches.kll.KllDoublesSketch $sketch = org.apache.datasketches.kll.KllDoublesSketch.wrap(org.apache.datasketches.memory.Memory.wrap($bytes));
         |$sketch.getMinItem();
       """.stripMargin
    ev.copy(code = CodeBlock(Seq(code), Seq.empty))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
  */
}

case class KllGetMax(child: Expression)
 extends UnaryExpression
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

  override protected def nullSafeCodeGen(ctx: CodegenContext, ev: ExprCode, f: String => String): ExprCode = {
    println("******\nChild type: " + child.toString() + "\n******")
    val childEval = child.genCode(ctx)
    println("******\nChild eval: " + childEval.toString() + "\n******")
    val sketch = ctx.freshName("sketch")
    println("******\nSketch name: " + sketch + "\n******")
    val code =
      s"""
         |${childEval.code}
         |final org.apache.datasketches.kll.KllDoublesSketch $sketch = org.apache.datasketches.kll.KllDoublesSketch.wrap(org.apache.datasketches.memory.Memory.wrap(${childEval.value}));
         |${ev.value} = $sketch.getMaxItem();
       """.stripMargin
    println(s"Generated code: $code")
    ev.copy(code = CodeBlock(Seq(code), Seq.empty), isNull = childEval.isNull)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => s"($c)")
  }
}