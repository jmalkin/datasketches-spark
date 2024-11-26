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

package org.apache.spark.sql.types

//import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.datasketches.memory.{WritableMemory, DefaultMemoryRequestServer}
import org.apache.datasketches.kll.KllDoublesSketch

class KllDoublesSketchType extends UserDefinedType[KllDoublesSketch] {
  override def sqlType: DataType = DataTypes.BinaryType

  override def serialize(sketch: KllDoublesSketch): Array[Byte] = {
    sketch.toByteArray
  }

  // assuming wrap is ok since read-only and already compact
  override def deserialize(data: Any): KllDoublesSketch = {
    val bytes = data.asInstanceOf[Array[Byte]]
    KllDoublesSketch.writableWrap(WritableMemory.writableWrap(bytes), new DefaultMemoryRequestServer())
  }

  // ensure we return the class type when queried
  override def userClass: Class[KllDoublesSketch] = classOf[KllDoublesSketch]

  override def catalogString: String = "KllDoublesSketch"
}

case object KllDoublesSketchType extends KllDoublesSketchType
