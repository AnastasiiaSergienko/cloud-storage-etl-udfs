package com.exasol.cloudetl.orc

import scala.collection.JavaConverters._

import com.exasol.cloudetl.data.Row

import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.orc.Reader
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

/**
 * An object class that creates an Orc [[com.exasol.cloudetl.data.Row]]
 * iterator from a provided Orc file [[org.apache.orc.Reader]].
 */
object OrcRowIterator {

  def apply(reader: Reader): Iterator[Seq[Row]] = new Iterator[Seq[Row]] {
    val readerBatch = reader.getSchema().createRowBatch()
    val readerRows = reader.rows(new Reader.Options())
    val structColumnVector = new StructColumnVector(readerBatch.numCols, readerBatch.cols: _*)
    val structDeserializer = new StructDeserializer(reader.getSchema.getChildren.asScala)

    override def hasNext: Boolean =
      readerRows.nextBatch(readerBatch) && !readerBatch.endOfFile && readerBatch.size > 0

    override def next(): Seq[Row] = {
      val size = readerBatch.size
      val rows = Vector.newBuilder[Row]
      for { rowIdx <- 0 until size } {
        val values = structDeserializer.readAt(structColumnVector, rowIdx)
        rows += Row(values.toSeq)
      }
      readerBatch.reset()
      rows.result()
    }
  }

}

object OrcDeserializer {

  def apply(orcType: TypeDescription): OrcDeserializer[_ <: ColumnVector] =
    orcType.getCategory match {
      case Category.BOOLEAN   => BooleanDeserializer
      case Category.CHAR      => StringDeserializer
      case Category.STRING    => StringDeserializer
      case Category.VARCHAR   => StringDeserializer
      case Category.DATE      => LongDeserializer
      case Category.DECIMAL   => DecimalDeserializer
      case Category.DOUBLE    => DoubleDeserializer
      case Category.FLOAT     => FloatDeserializer
      case Category.LONG      => LongDeserializer
      case Category.INT       => IntDeserializer
      case Category.SHORT     => IntDeserializer
      case Category.TIMESTAMP => TimestampDeserializer
      case Category.LIST =>
        throw new IllegalArgumentException("Orc list type is not supported.")
      case Category.MAP =>
        throw new IllegalArgumentException("Orc map type is not supported.")
      case Category.STRUCT =>
        throw new IllegalArgumentException("Orc nested struct type is not supported.")
      case _ =>
        throw new IllegalArgumentException(
          s"Found orc unsupported type, '${orcType.getCategory}'."
        )
    }

}

/**
 * An interface for all type deserializers.
 */
sealed trait OrcDeserializer[T <: ColumnVector] {

  /**
   * Reads the row at provided index from the vector.
   *
   * @param vector The Orc
   *        [[org.apache.hadoop.hive.ql.exec.vector.ColumnVector]] vector
   * @param index The index to read at
   */
  def readAt(vector: T, index: Int): Any
}

final class StructDeserializer(fieldTypes: Seq[TypeDescription])
    extends OrcDeserializer[StructColumnVector] {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def readFromColumn[T <: ColumnVector](
    struct: StructColumnVector,
    rowIndex: Int,
    columnIndex: Int
  ): Any = {
    val deserializer = OrcDeserializer(fieldTypes(columnIndex)).asInstanceOf[OrcDeserializer[T]]
    val vector = struct.fields(columnIndex).asInstanceOf[T]
    val newRowIndex = if (vector.isRepeating) 0 else rowIndex
    deserializer.readAt(vector, newRowIndex)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Nothing"))
  override final def readAt(vector: StructColumnVector, rowIndex: Int): Seq[Any] = {
    val size = fieldTypes.size
    val values = Array.ofDim[Any](size)
    for { columnIndex <- 0 until size } {
      values.update(columnIndex, readFromColumn(vector, rowIndex, columnIndex))
    }
    values.toSeq
  }

}

object BooleanDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readAt(vector: LongColumnVector, index: Int): Boolean =
    vector.vector(index) == 1
}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object IntDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readAt(vector: LongColumnVector, index: Int): Any =
    if (vector.isNull(index)) {
      null // scalastyle:ignore null
    } else {
      vector.vector(index).toInt
    }
}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object LongDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readAt(vector: LongColumnVector, index: Int): Any =
    if (vector.isNull(index)) {
      null // scalastyle:ignore null
    } else {
      vector.vector(index)
    }
}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object DoubleDeserializer extends OrcDeserializer[DoubleColumnVector] {
  override def readAt(vector: DoubleColumnVector, index: Int): Any =
    if (vector.isNull(index)) {
      null // scalastyle:ignore null
    } else {
      vector.vector(index)
    }
}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object FloatDeserializer extends OrcDeserializer[DoubleColumnVector] {
  override def readAt(vector: DoubleColumnVector, index: Int): Any =
    if (vector.isNull(index)) {
      null // scalastyle:ignore null
    } else {
      vector.vector(index).toFloat
    }
}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object TimestampDeserializer extends OrcDeserializer[TimestampColumnVector] {
  override def readAt(vector: TimestampColumnVector, index: Int): java.sql.Timestamp =
    if (vector.isNull(index)) {
      null // scalastyle:ignore null
    } else {
      new java.sql.Timestamp(vector.getTime(index))
    }
}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object DecimalDeserializer extends OrcDeserializer[DecimalColumnVector] {
  override def readAt(vector: DecimalColumnVector, index: Int): BigDecimal =
    if (vector.isNull(index)) {
      null // scalastyle:ignore null
    } else {
      BigDecimal(vector.vector(index).getHiveDecimal.bigDecimalValue)
    }
}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object StringDeserializer extends OrcDeserializer[BytesColumnVector] {
  override def readAt(vector: BytesColumnVector, index: Int): Any =
    if (vector.isNull(index)) {
      null // scalastyle:ignore null
    } else {
      try {
        val bytes = vector.vector.headOption.fold(Array.empty[Byte])(
          _.slice(vector.start(index), vector.start(index) + vector.length(index))
        )
        new String(bytes, "UTF8")
      } catch {
        case e: Exception =>
          throw e
      }
    }
}
