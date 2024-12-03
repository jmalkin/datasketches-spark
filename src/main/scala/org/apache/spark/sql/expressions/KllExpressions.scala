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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpectsInputTypes, UnaryExpression}
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, KllDoublesSketchType}
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
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
}

case class KllGetMax(child: Expression)
 extends UnaryExpression
 with CodegenFallback
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