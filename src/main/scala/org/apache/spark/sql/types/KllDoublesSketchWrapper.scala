package org.apache.spark.sql.types

import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.memory.{WritableMemory, DefaultMemoryRequestServer}

@SQLUserDefinedType(udt = classOf[KllDoublesSketchType])
case class KllDoublesSketchWrapper(sketch: KllDoublesSketch) extends Serializable {
  //def this() = this(KllDoublesSketch.newHeapInstance())

  def toByteArray: Array[Byte] = sketch.toByteArray

  def toString(withLevels: Boolean = false, withData: Boolean = false ): String = {
    sketch.toString(withLevels, withData)
  }
}

object KllDoublesSketchWrapper {
  //implicit val kllDoublesSketchSerializer: KllDoublesSketch => Array[Byte] = _.toByteArray
  //implicit val kllDoublesSketchDeserializer: Array[Byte] => KllDoublesSketch = bytes => {
  //  KllDoublesSketch.writableWrap(WritableMemory.writableWrap(bytes), new DefaultMemoryRequestServer())
  //}

  // not used now but we may want to use a generic wrapper later
  //type KllDoublesSketchWrapper = GenericWrapper[KllDoublesSketch]
  //val KllDoublesSketchWrapperType: GenericWrapperType[KllDoublesSketch] = GenericWrapperType[KllDoublesSketch]

  // TODO: determine if heapify is better
  def deserialize(bytes: Array[Byte]): KllDoublesSketchWrapper = {
    val sketch = KllDoublesSketch.writableWrap(WritableMemory.writableWrap(bytes), new DefaultMemoryRequestServer())
    KllDoublesSketchWrapper(sketch)
  }
}