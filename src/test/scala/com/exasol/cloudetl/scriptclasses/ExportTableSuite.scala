package com.exasol.cloudetl.scriptclasses

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.exasol.ExaIterator
import com.exasol.ExaMetadata

import org.mockito.ArgumentMatchers.any
import org.mockito.ExtraMockito
import org.mockito.Mockito._

@SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
class ExportTableSuite extends BaseSuite {

  val srcColumns: Seq[String] = Seq(
    "c_int",
    "c_long",
    "c_decimal",
    "c_double",
    "c_string",
    "c_boolean",
    "c_date",
    "c_timestamp"
  )

  private val bd1 = new java.math.BigDecimal("5555555555555555555555555555555.55555")
  private val bd2 = new java.math.BigDecimal("5555555555555555555555555555555.55555")
  private val dt1 = new java.sql.Date(System.currentTimeMillis())
  private val dt2 = new java.sql.Date(System.currentTimeMillis())
  private val ts1 = new java.sql.Timestamp(System.currentTimeMillis())
  private val ts2 = new java.sql.Timestamp(System.currentTimeMillis())

  val records: Seq[Seq[Object]] = Seq(
    Seq(1, 3L, bd1, 3.14d, "xyz", true, dt1, ts1),
    Seq(2, 4L, bd2, 0.13d, "abc", false, dt2, ts2)
  ).map { seq =>
    seq.map(_.asInstanceOf[AnyRef])
  }

  final def createMockedIter(resourceDir: String): ExaIterator = {
    val mockedIter = commonExaIterator(resourceDir)
    when(mockedIter.getString(2)).thenReturn(srcColumns.mkString("."))
    when(mockedIter.next()).thenReturn(true, false)

    when(mockedIter.getInteger(3)).thenReturn(1, 2)
    when(mockedIter.getLong(4)).thenReturn(3L, 4L)
    when(mockedIter.getBigDecimal(5)).thenReturn(bd1, bd2)
    when(mockedIter.getDouble(6)).thenReturn(3.14, 0.13)
    when(mockedIter.getString(7)).thenReturn("xyz", "abc")
    when(mockedIter.getBoolean(8)).thenReturn(true, false)
    when(mockedIter.getDate(9)).thenReturn(dt1, dt2)
    when(mockedIter.getTimestamp(10)).thenReturn(ts1, ts2)

    mockedIter
  }

  final def createMockedMeta(): ExaMetadata = {
    val mockedMeta = mock[ExaMetadata]
    when(mockedMeta.getInputColumnCount()).thenReturn(11L)
    val returns = Seq(
      (3, classOf[java.lang.Integer], 0L, 0L, 0L),
      (4, classOf[java.lang.Long], 0L, 0L, 0L),
      (5, classOf[java.math.BigDecimal], 36L, 5L, 0L),
      (6, classOf[java.lang.Double], 0L, 0L, 0L),
      (7, classOf[java.lang.String], 0L, 0L, 3L),
      (8, classOf[java.lang.Boolean], 0L, 0L, 0L),
      (9, classOf[java.sql.Date], 0L, 0L, 0L),
      (10, classOf[java.sql.Timestamp], 0L, 0L, 0L)
    )
    returns.foreach {
      case (idx, cls, prec, scale, len) =>
        ExtraMockito.doReturn(cls).when(mockedMeta).getInputColumnType(idx)
        when(mockedMeta.getInputColumnPrecision(idx)).thenReturn(prec)
        when(mockedMeta.getInputColumnScale(idx)).thenReturn(scale)
        when(mockedMeta.getInputColumnLength(idx)).thenReturn(len)
    }

    mockedMeta
  }

  test("`run` should export the Exasol rows from ExaIterator") {
    val tempDir = Files.createTempDirectory("exportTableTest")

    val meta = createMockedMeta()
    val iter = createMockedIter(tempDir.toUri.toString)

    ExportTable.run(meta, iter)

    verify(meta, times(1)).getInputColumnCount
    for { idx <- 3 to 10 } {
      verify(meta, times(1)).getInputColumnType(idx)
      verify(meta, times(1)).getInputColumnPrecision(idx)
      verify(meta, times(1)).getInputColumnScale(idx)
      verify(meta, times(1)).getInputColumnLength(idx)
    }

    verify(iter, times(2)).getInteger(3)
    verify(iter, times(2)).getLong(4)
    verify(iter, times(2)).getBigDecimal(5)
    verify(iter, times(2)).getDouble(6)
    verify(iter, times(2)).getString(7)
    verify(iter, times(2)).getBoolean(8)
    verify(iter, times(2)).getDate(9)
    verify(iter, times(2)).getTimestamp(10)

    deleteFiles(tempDir)
  }

  test("import exported rows from a file") {
    val tempDir = Files.createTempDirectory("importExportTableTest")
    val meta = createMockedMeta()
    val iter = createMockedIter(tempDir.toUri.toString)

    ExportTable.run(meta, iter)

    val importIter = commonExaIterator(resourceImportBucket)
    when(importIter.next()).thenReturn(false)
    when(importIter.getString(2)).thenReturn(tempDir.toUri.toString)

    ImportFiles.run(mock[ExaMetadata], importIter)

    val totalRecords = 2
    verify(importIter, times(totalRecords)).emit(Seq(any[Object]): _*)

    // TODO: verify each emitted row

    deleteFiles(tempDir)
  }

  final def deleteFiles(dir: Path): Unit = {
    Files.walkFileTree(
      dir,
      new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }
        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }
    )
    ()
  }

}