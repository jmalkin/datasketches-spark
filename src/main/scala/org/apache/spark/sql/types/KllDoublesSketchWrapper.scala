package org.apache.spark.sql.types

import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.memory.Memory

@SQLUserDefinedType(udt = classOf[KllDoublesSketchType])
case class KllDoublesSketchWrapper(sketch: KllDoublesSketch) extends Serializable {

  def toByteArray: Array[Byte] = sketch.toByteArray

  def serialize: Array[Byte] = sketch.toByteArray

  def toString(withLevels: Boolean = false, withData: Boolean = false ): String = {
    sketch.toString(withLevels, withData)
  }
}

object KllDoublesSketchWrapper {
  def deserialize(bytes: Array[Byte]): KllDoublesSketchWrapper = {
    //val sketch = KllDoublesSketch.writableWrap(WritableMemory.writableWrap(bytes), new DefaultMemoryRequestServer())
    val sketch = KllDoublesSketch.heapify(Memory.wrap(bytes))
    KllDoublesSketchWrapper(sketch)
  }
}