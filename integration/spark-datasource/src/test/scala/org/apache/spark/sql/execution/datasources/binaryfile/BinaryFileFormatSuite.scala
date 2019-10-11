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

package org.apache.spark.sql.execution.datasources.binaryfile

import java.io.{File, IOException}
import java.nio.file.{Files, StandardOpenOption}
import java.sql.Timestamp

import scala.collection.JavaConverters._

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs.{FileStatus, FileSystem, GlobFilter, Path}
import org.mockito.Mockito.{mock, when}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.carbondata.datasource.TestUtil._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import BinaryFileFormat.SOURCES_BINARY_FILE_MAX_LENGTH

class BinaryFileFormatSuite extends FunSuite with BeforeAndAfterAll {

  import BinaryFileFormat._

  private var testDir: String = _

  private var fsTestDir: Path = _

  private var fs: FileSystem = _

  private var file1Status: FileStatus = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    testDir = Utils.createTempDir().getAbsolutePath
    fsTestDir = new Path(testDir)
    fs = fsTestDir.getFileSystem(spark.sparkContext.hadoopConfiguration)

    val year2014Dir = new File(testDir, "year=2014")
    year2014Dir.mkdir()
    val year2015Dir = new File(testDir, "year=2015")
    year2015Dir.mkdir()

    val file1 = new File(year2014Dir, "data.txt")
    Files.write(
      file1.toPath,
      Seq("2014-test").asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
    file1Status = fs.getFileStatus(new Path(file1.getPath))

    val file2 = new File(year2014Dir, "data2.bin")
    Files.write(
      file2.toPath,
      "2014-test-bin".getBytes,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file3 = new File(year2015Dir, "bool.csv")
    Files.write(
      file3.toPath,
      Seq("bool", "True", "False", "true").asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )

    val file4 = new File(year2015Dir, "data.bin")
    Files.write(
      file4.toPath,
      "2015-test".getBytes,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE
    )
  }

  test("BinaryFileFormat methods") {
    val format = new BinaryFileFormat
    assert(format.shortName() === "binaryFile")
    assert(format.isSplitable(spark, Map.empty, new Path("any")) === false)
    assert(format.inferSchema(spark, Map.empty, Seq.empty) === Some(BinaryFileFormat.schema))
    assert(BinaryFileFormat.schema === StructType(Seq(
      StructField("path", StringType, false),
      StructField("modificationTime", TimestampType, false),
      StructField("length", LongType, false),
      StructField("content", BinaryType, true))))
  }

  def testBinaryFileDataSource(pathGlobFilter: String): Unit = {
    val dfReader = spark.read.format(BINARY_FILE)
    if (pathGlobFilter != null) {
      // only supported in spark 3.0
      dfReader.option("pathGlobFilter", pathGlobFilter)
    }
    val resultDFTemp = dfReader.load(testDir)
    val res = resultDFTemp.collect()
    val resultDF =   resultDFTemp.select(
        col(PATH),
        col(MODIFICATION_TIME),
        col(LENGTH),
        col(CONTENT),
        col("year") // this is a partition column
      )

    val expectedRowSet = new collection.mutable.HashSet[Row]()

    val globFilter = if (pathGlobFilter == null) null else new GlobFilter(pathGlobFilter)
    for (partitionDirStatus <- fs.listStatus(fsTestDir)) {
      val dirPath = partitionDirStatus.getPath

      val partitionName = dirPath.getName.split("=")(1)
      val year = partitionName.toInt // partition column "year" value which is `Int` type

      for (fileStatus <- fs.listStatus(dirPath)) {
        if (globFilter == null || globFilter.accept(fileStatus.getPath)) {
          val fpath = fileStatus.getPath.toString
          val flen = fileStatus.getLen
          val modificationTime = new Timestamp(fileStatus.getModificationTime)

          val fcontent = {
            val stream = fs.open(fileStatus.getPath)
            val content = try {
              ByteStreams.toByteArray(stream)
            } finally {
              Closeables.close(stream, true)
            }
            content
          }

          val row = Row(fpath, modificationTime, flen, fcontent, year)
          expectedRowSet.add(row)
        }
      }
    }

    checkAnswer(resultDF, expectedRowSet.toSeq)
  }

  test("binary file data source test") {
    testBinaryFileDataSource(null)
    testBinaryFileDataSource("*.*")
    // following filters using pathGlobFilter only supported in Spark 3.0
    // testBinaryFileDataSource("*.bin")
    // testBinaryFileDataSource("*.txt")
    // testBinaryFileDataSource("*.{txt,csv}")
    // testBinaryFileDataSource("*.json")
  }

  test("binary file data source do not support write operation") {
    val df = spark.read.format(BINARY_FILE).load(testDir)
    withTempDir { tmpDir =>
      val thrown = intercept[UnsupportedOperationException] {
        df.write
          .format(BINARY_FILE)
          .save(tmpDir + "/test_save")
      }
      assert(thrown.getMessage.contains("Write is not supported for binary file data source"))
    }
  }

  def mockFileStatus(length: Long, modificationTime: Long): FileStatus = {
    val status = mock(classOf[FileStatus])
    when(status.getLen).thenReturn(length)
    when(status.getModificationTime).thenReturn(modificationTime)
    when(status.toString).thenReturn(
      s"FileStatus($LENGTH=$length, $MODIFICATION_TIME=$modificationTime)")
    status
  }

  def testCreateFilterFunction(
      filters: Seq[Filter],
      testCases: Seq[(FileStatus, Boolean)]): Unit = {
    val funcs = filters.map(BinaryFileFormat.createFilterFunction)
    testCases.foreach { case (status, expected) =>
      assert(funcs.forall(f => f(status)) === expected,
        s"$filters applied to $status should be $expected.")
    }
  }

  test("createFilterFunction") {
    // test filter applied on `length` column
    val l1 = mockFileStatus(1L, 0L)
    val l2 = mockFileStatus(2L, 0L)
    val l3 = mockFileStatus(3L, 0L)
    testCreateFilterFunction(
      Seq(LessThan(LENGTH, 2L)),
      Seq((l1, true), (l2, false), (l3, false)))
    testCreateFilterFunction(
      Seq(LessThanOrEqual(LENGTH, 2L)),
      Seq((l1, true), (l2, true), (l3, false)))
    testCreateFilterFunction(
      Seq(GreaterThan(LENGTH, 2L)),
      Seq((l1, false), (l2, false), (l3, true)))
    testCreateFilterFunction(
      Seq(GreaterThanOrEqual(LENGTH, 2L)),
      Seq((l1, false), (l2, true), (l3, true)))
    testCreateFilterFunction(
      Seq(EqualTo(LENGTH, 2L)),
      Seq((l1, false), (l2, true), (l3, false)))
    testCreateFilterFunction(
      Seq(Not(EqualTo(LENGTH, 2L))),
      Seq((l1, true), (l2, false), (l3, true)))
    testCreateFilterFunction(
      Seq(And(GreaterThan(LENGTH, 1L), LessThan(LENGTH, 3L))),
      Seq((l1, false), (l2, true), (l3, false)))
    testCreateFilterFunction(
      Seq(Or(LessThanOrEqual(LENGTH, 1L), GreaterThanOrEqual(LENGTH, 3L))),
      Seq((l1, true), (l2, false), (l3, true)))

    // test filter applied on `modificationTime` column
    val t1 = mockFileStatus(0L, 1L)
    val t2 = mockFileStatus(0L, 2L)
    val t3 = mockFileStatus(0L, 3L)
    testCreateFilterFunction(
      Seq(LessThan(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, true), (t2, false), (t3, false)))
    testCreateFilterFunction(
      Seq(LessThanOrEqual(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, true), (t2, true), (t3, false)))
    testCreateFilterFunction(
      Seq(GreaterThan(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, false), (t2, false), (t3, true)))
    testCreateFilterFunction(
      Seq(GreaterThanOrEqual(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, false), (t2, true), (t3, true)))
    testCreateFilterFunction(
      Seq(EqualTo(MODIFICATION_TIME, new Timestamp(2L))),
      Seq((t1, false), (t2, true), (t3, false)))
    testCreateFilterFunction(
      Seq(Not(EqualTo(MODIFICATION_TIME, new Timestamp(2L)))),
      Seq((t1, true), (t2, false), (t3, true)))
    testCreateFilterFunction(
      Seq(And(GreaterThan(MODIFICATION_TIME, new Timestamp(1L)),
        LessThan(MODIFICATION_TIME, new Timestamp(3L)))),
      Seq((t1, false), (t2, true), (t3, false)))
    testCreateFilterFunction(
      Seq(Or(LessThanOrEqual(MODIFICATION_TIME, new Timestamp(1L)),
        GreaterThanOrEqual(MODIFICATION_TIME, new Timestamp(3L)))),
      Seq((t1, true), (t2, false), (t3, true)))

    // test filters applied on both columns
    testCreateFilterFunction(
      Seq(And(GreaterThan(LENGTH, 2L), LessThan(MODIFICATION_TIME, new Timestamp(2L)))),
      Seq((l1, false), (l2, false), (l3, true), (t1, false), (t2, false), (t3, false)))

    // test nested filters
    testCreateFilterFunction(
      // NOT (length > 2 OR modificationTime < 2)
      Seq(Not(Or(GreaterThan(LENGTH, 2L), LessThan(MODIFICATION_TIME, new Timestamp(2L))))),
      Seq((l1, false), (l2, false), (l3, false), (t1, false), (t2, true), (t3, true)))
  }

  test("buildReader") {
    def testBuildReader(fileStatus: FileStatus, filters: Seq[Filter], expected: Boolean): Unit = {
      val format = new BinaryFileFormat
      val reader = format.buildReaderWithPartitionValues(
        sparkSession = spark,
        dataSchema = schema,
        partitionSchema = StructType(Nil),
        requiredSchema = schema,
        filters = filters,
        options = Map.empty,
        hadoopConf = spark.sessionState.newHadoopConf())
      val partitionedFile = mock(classOf[PartitionedFile])
      when(partitionedFile.filePath).thenReturn(fileStatus.getPath.toString)
      assert(reader(partitionedFile).nonEmpty === expected,
        s"Filters $filters applied to $fileStatus should be $expected.")
    }
    testBuildReader(file1Status, Seq.empty, true)
    testBuildReader(file1Status, Seq(LessThan(LENGTH, file1Status.getLen)), false)
    testBuildReader(file1Status, Seq(
      LessThan(MODIFICATION_TIME, new Timestamp(file1Status.getModificationTime))
    ), false)
    testBuildReader(file1Status, Seq(
      EqualTo(LENGTH, file1Status.getLen),
      EqualTo(MODIFICATION_TIME, file1Status.getModificationTime)
    ), true)
  }

  ignore("column pruning") {
    def getRequiredSchema(fieldNames: String*): StructType = {
      StructType(fieldNames.map {
        case f if schema.fieldNames.contains(f) => schema(f)
        case other => StructField(other, NullType)
      })
    }
    def read(file: File, requiredSchema: StructType): Row = {
      val format = new BinaryFileFormat
      val reader = format.buildReaderWithPartitionValues(
        sparkSession = spark,
        dataSchema = schema,
        partitionSchema = StructType(Nil),
        requiredSchema = requiredSchema,
        filters = Seq.empty,
        options = Map.empty,
        hadoopConf = spark.sessionState.newHadoopConf()
      )
      val partitionedFile = mock(classOf[PartitionedFile])
      when(partitionedFile.filePath).thenReturn(file.getPath)
      val encoder = RowEncoder(requiredSchema).resolveAndBind()
      encoder.fromRow(reader(partitionedFile).next())
    }
    val file = new File(Utils.createTempDir(), "data")
    val content = "123".getBytes
    Files.write(file.toPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE)

    read(file, getRequiredSchema(MODIFICATION_TIME, CONTENT, LENGTH, PATH)) match {
      case Row(t, c, len, p) =>
        assert(t === new Timestamp(file.lastModified()))
        assert(c === content)
        assert(len === content.length)
        assert(p.asInstanceOf[String].endsWith(file.getAbsolutePath))
    }
    file.setReadable(false)
    withClue("cannot read content") {
      intercept[IOException] {
        read(file, getRequiredSchema(CONTENT))
      }
    }
    assert(read(file, getRequiredSchema(LENGTH)) === Row(content.length),
      "Get length should not read content.")
    intercept[RuntimeException] {
      read(file, getRequiredSchema(LENGTH, "other"))
    }

    val df = spark.read.format(BINARY_FILE).load(file.getPath)
    assert(df.count() === 1, "Count should not read content.")
    assert(df.select("LENGTH").first().getLong(0) === content.length,
      "column pruning should be case insensitive")
  }

  def withTempDir(f: File => Unit): Unit = {
    var tempDir : File = null
    try {
      tempDir = Utils.createTempDir()
    f(tempDir)
  } finally {
    if(tempDir != null) {
      tempDir.delete()
    }
  }
  }

  def withTempPath(f: File => Unit): Unit = {
    var tempDir : File = null
    var tempfile : File = null
    try {
      tempDir = Utils.createTempDir()
      tempfile = new File(tempDir+"f1")
      f(tempfile)
    } finally {
      if(tempfile != null) {
        tempfile.delete()
      }
      if(tempDir != null) {
        tempDir.delete()
      }
    }
  }

  test("fail fast and do not attempt to read if a file is too big") {
    assert(spark.conf.get(SOURCES_BINARY_FILE_MAX_LENGTH) === Int.MaxValue)
    withTempPath { file =>
      val path = file.getPath
      val content = "123".getBytes
      Files.write(file.toPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      def readContent(): DataFrame = {
        spark.read.format(BINARY_FILE)
          .load(path)
          .select(CONTENT)
      }
      val expected = Seq(Row(content))
      checkAnswer(readContent(), expected)
    }
  }

  test("SPARK-28030: support chars in file names that require URL encoding") {
    withTempDir { dir =>
      val file = new File(dir, "test space.txt")
      val content = "123".getBytes
      Files.write(file.toPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      val df = spark.read.format(BINARY_FILE).load(dir.getPath)
      df.select(col(PATH), col(CONTENT)).first() match {
        case Row(p: String, c: Array[Byte]) =>
          assert(p.endsWith(file.getAbsolutePath), "should support space in file name")
          assert(c === content, "should read file with space in file name")
      }
    }
  }

  test("support reading binary file using sql syntax") {
    withTempDir { dir =>
      val file = new File(dir, "test space.txt")
      val content = "123".getBytes
      Files.write(file.toPath, content, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      spark.sql("create temporary table t1 using binaryfile options(path='"
        + dir.getPath + "')" )
      val df = spark.sql("select * from t1")
      df.select(col(PATH), col(CONTENT)).first() match {
        case Row(p: String, c: Array[Byte]) =>
          assert(p.endsWith(file.getAbsolutePath), "should support space in file name")
          assert(c === content, "should read file with space in file name")
      }
    }
  }
}
