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

package org.apache.spark.sql.execution.command.vector

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SparkContext, TaskContext, TaskKilledException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, CarbonDatasourceHadoopRelation, Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.{CarbonParserUtil, InternalRow}
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.command.{BucketFields, Field, TableNewProcessor}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.json.JsonInferSchema
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, ColumnSchema}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit}
import org.apache.carbondata.spark.rdd.CarbonSparkPartition
import org.apache.carbondata.spark.util.{DataTypeConverterUtil, Util}
import org.apache.carbondata.vector.VectorTableInputFormat
import org.apache.carbondata.vector.column.{VectorColumnReader, VectorColumnWriter}
import org.apache.carbondata.vector.table.VectorTablePath

/**
 * Utility functions for INSERT COLUMN execution
 */
object InsertColumnsHelper {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * insert data of columns
   */
  def insertColumnsForVectorTable(
      sparkSession: SparkSession,
      table: CarbonTable,
      fields: Seq[Field],
      input: DataFrame,
      hadoopConf: Configuration): Seq[ColumnSchema] = {
    val plan = input.logicalPlan
    validateInput(sparkSession, table, plan, fields, true)
    val optimizedPlan = optimizePlan(plan, fields)
    // double check after optimizing
    validateInput(sparkSession, table, optimizedPlan, fields, false)
    val newInput = SparkSQLUtil.ofRows(sparkSession, optimizedPlan)
    val fieldsWithInferNeeded = needInferSchema(fields, optimizedPlan)
    val (columns, columnSchemas) = fieldsWithInferNeeded.exists(_._2) match {
      case true =>
        inferSchemaAndInsertColumn(
          sparkSession,
          table,
          fieldsWithInferNeeded,
          newInput,
          hadoopConf)
      case false =>
        insertColumnsJob(
          sparkSession,
          table,
          fieldsWithInferNeeded.map(_._1),
          newInput,
          hadoopConf)
    }
    // update schema ordinal
    var size = table.getCreateOrderColumn(table.getTableName).size()
    columns.foreach { column =>
      column.getColumnSchema.setSchemaOrdinal(size)
      size += 1
    }
    columnSchemas
  }

  /**
   * run job to insert columns
   */
  def insertColumnsJob(
      sparkSession: SparkSession,
      table: CarbonTable,
      fields: Seq[Field],
      input: DataFrame,
      hadoopConf: Configuration
  ): (Seq[CarbonColumn], Seq[ColumnSchema]) = {
    val (columns, columnSchemas) = generateVirtualColumn(fields)
    // write columns data for each segment
    val serializedConf = SparkSQLUtil.getSerializableConfigurableInstance(hadoopConf)
    var hasException = false
    val segments = VectorTableInputFormat.getSegments(table, hadoopConf)
    try {
      val tableInfo = table.getTableInfo
      input
        .rdd
        .mapPartitions { iterator =>
          val segmentNo = VectorTableInputFormat.getSegmentNo()
          if (segmentNo == null) {
            // no need retry task
            throw new RuntimeException("Not supported this query")
          }
          writeColumnsTask(
            tableInfo,
            columns.toArray,
            segmentNo,
            iterator,
            serializedConf.value
          )
        }.collect()
      (columns, columnSchemas)
    } catch {
      case e =>
        hasException = true
        throw e
    } finally {
      if (hasException) {
        columns.foreach { column =>
          deleteTempColumnFiles(table, segments, column, hadoopConf)
        }
      }
    }
  }

  /**
   * run a task to write columns into the table for a segment
   * now only support writing one column at once
   */
  def writeColumnsTask(
      tableInfo: TableInfo,
      columns: Array[CarbonColumn],
      segmentNo: String,
      iterator: Iterator[Row],
      hadoopConf: Configuration
  ): Iterator[Row] = {
    val table = CarbonTable.buildFromTableInfo(tableInfo)
    val writer = new VectorColumnWriter(table, columns, segmentNo, hadoopConf)
    try {
      iterator.foreach { value =>
        writer.write(value.toSeq.toArray[Any].asInstanceOf[Array[Object]])
      }
    } finally {
      writer.close()
    }
    Iterator.empty
  }

  /**
   * infer schema and insert data of column
   */
  def inferSchemaAndInsertColumn(
      sparkSession: SparkSession,
      table: CarbonTable,
      fields: Seq[(Field, Boolean)],
      input: DataFrame,
      hadoopConf: Configuration
  ): (Seq[CarbonColumn], Seq[ColumnSchema]) = {
    // generate field list for insert data
    val virtualFields =
      fields.map { field =>
        if (field._2) {
          val fieldName = UUID.randomUUID().toString
          new Field(fieldName, Option("string"), Option(fieldName), None)
        } else {
          field._1
        }
      }
    // insert data of columns
    val (columns, columnSchemas) =
      insertColumnsJob(
        sparkSession,
        table,
        virtualFields,
        input,
        hadoopConf)
    // group by column schema list
    val columnGroups = groupColumnSchema(columns, columnSchemas)
    // infer schema
    val partitions = getPartitions(table, hadoopConf).toArray
    val finalColumnGroups = (0 until fields.length).map { index =>
      if (fields(index)._2) {
        inferJsonSchemaForOneColumn(
          sparkSession,
          table,
          columns(index),
          fields(index)._1,
          partitions,
          hadoopConf
        )
      } else {
        columnGroups(index)
      }
    }
    (finalColumnGroups.map(_._1), finalColumnGroups.flatMap(_._2))
  }

  /**
   * infer the schema for a json column
   */
  def inferJsonSchemaForOneColumn(
      sparkSession: SparkSession,
      table: CarbonTable,
      column: CarbonColumn,
      field: Field,
      partitions: Array[Partition],
      hadoopConf: Configuration
  ): (CarbonColumn, Seq[ColumnSchema]) = {
    val jsonRDD = new ColumnScanRDD(
      table.getTableInfo,
      column,
      partitions,
      hadoopConf,
      sparkSession.sparkContext)
    val dataType = inferSchema(sparkSession, jsonRDD)
    insertJsonJob(
      sparkSession,
      table,
      jsonRDD,
      updateField(field, dataType),
      dataType,
      hadoopConf)
  }

  /**
   * infer the schema of json
   */
  def inferSchema(sparkSession: SparkSession, jsonRDD: ColumnScanRDD): DataType = {
    val parsedOptions = new JSONOptions(
      Map.empty[String, String],
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    val schema = JsonInferSchema.infer(
      jsonRDD.map(_.getUTF8String(0)),
      parsedOptions,
      CreateJacksonParser.utf8String)

    schema
  }

  def updateField(field: Field, dataType: DataType): Field = {
    val schema = Seq(StructField(field.column, dataType))
    val parser = new CarbonSpark2SqlParser()
    val finalFields = parser.getFields(schema)
    finalFields(0)
  }

  def insertJsonJob(sparkSession: SparkSession,
      table: CarbonTable,
      scanRDD: ColumnScanRDD,
      field: Field,
      dataType: DataType,
      hadoopConf: Configuration
  ): (CarbonColumn, Seq[ColumnSchema]) = {
    val (finalColumns, finalColumnSchemas) = generateVirtualColumn(Seq(field))
    // convert json string to struct
    val tableInfo = table.getTableInfo
    val serializedConf = SparkSQLUtil.getSerializableConfigurableInstance(hadoopConf)
    import org.apache.spark.sql.functions.from_json
    SparkSQLUtil
      .internalCreateDataFrame(
        sparkSession,
        scanRDD,
        StructType(Seq(StructField("col1", StringType))))
      .select(from_json(Column("col1"), dataType, new util.HashMap[String, String]()))
      .rdd
      .mapPartitions { iterator =>
        val segmentNo = VectorTableInputFormat.getSegmentNo()
        if (segmentNo == null) {
          // no need retry task
          throw new RuntimeException("Not supported this query")
        }
        writeColumnsTask(
          tableInfo,
          finalColumns.toArray,
          segmentNo,
          iterator,
          serializedConf.value
        )
      }.collect()
    (finalColumns(0), finalColumnSchemas)
  }

  def getPartitions(
      table: CarbonTable,
      hadoopConf: Configuration
  ): Seq[Partition] = {
    VectorTableInputFormat
      .getSplit(table, hadoopConf)
      .asScala
      .zipWithIndex
      .map { case (split, index) =>
        new CarbonSparkPartition(
          0,
          index,
          new CarbonMultiBlockSplit(split.asInstanceOf[CarbonInputSplit])
        ).asInstanceOf[Partition]
      }
  }

  /**
   * group ColumnSchema list to move all child columns of a column into the same group.
   */
  def groupColumnSchema(
      columns: Seq[CarbonColumn],
      columnSchemas: Seq[ColumnSchema]
  ): Seq[(CarbonColumn, Seq[ColumnSchema])] = {
    if (columns.length == 1) {
      Seq((columns(0), columnSchemas))
    } else {
      val idMapGroup = new util.HashMap[String, ArrayBuffer[ColumnSchema]]()
      var group: ArrayBuffer[ColumnSchema] = null
      columnSchemas.map { columnSchema =>
        if (columnSchema.getSchemaOrdinal != -1) {
          group = new ArrayBuffer[ColumnSchema]()
          idMapGroup.put(columnSchema.getColumnUniqueId, group)
        }
        group += columnSchema
      }
      columns.map { column =>
        (column, idMapGroup.get(column.getColumnId))
      }
    }
  }

  /**
   * check whether it requires to infer schema for each field or not
   */
  def needInferSchema(fields: Seq[Field], inputPlan: LogicalPlan): Seq[(Field, Boolean)] = {
    val parser = new CarbonSpark2SqlParser()
    val projectList = inputPlan.asInstanceOf[Project].projectList
    fields.zipWithIndex.map { field =>
      val dataType = projectList(field._2).dataType.simpleString
      field._1.dataType match {
        case None =>
          val schema = Seq(StructField(field._1.column, projectList(field._2).dataType))
          (parser.getFields(schema)(0), false)
        case Some(fieldType) =>
          if (fieldType == "json" && dataType != "string") {
            throw new AnalysisException(
              "the query data type should be string when the column data type is json")
          }
          val isNeed = dataType == "string" &&
                       field._1.children.isEmpty &&
                       (fieldType == "json")
          (field._1, isNeed)
      }
    }
  }

  /**
   * generate virtual columns for insert columns function
   * it is not in the table schema now.
   */
  def generateVirtualColumn(field: Seq[Field]): (Seq[CarbonColumn], Seq[ColumnSchema]) = {
    // TODO need to infer data type for abstract complex data type
    val virtualTableModel = CarbonParserUtil.prepareTableModel(
      true,
      Option("default"),
      "default",
      field,
      Seq.empty,
      scala.collection.mutable.Map.empty[String, String],
      Option.empty[BucketFields],
      false,
      false,
      None)
    val virtualTableInfo = TableNewProcessor(virtualTableModel)
    val virtualTable = CarbonTable.buildFromTableInfo(virtualTableInfo)
    val virtualColumns =
      virtualTable
        .getCreateOrderColumn(virtualTable.getTableName)
    val virtualSchemas =
      virtualTable
        .getTableInfo
        .getFactTable
        .getListOfColumns
        .asScala
        .filter(!_.isInvisible)
    (virtualColumns.asScala, virtualSchemas)
  }

  /**
   * the sql syntax of the supported query:
   * select
   * from <table_name>
   * where ...
   *
   * not support: join, group by, aggregate and window function.
   */
  def validateInput(
      sparkSession: SparkSession,
      table: CarbonTable,
      inputPlan: LogicalPlan,
      fields: Seq[Field],
      hasFilter: Boolean): Unit = {

    def matchRelation(plan: LogicalPlan): Unit = {
      plan match {
        case SubqueryAlias(_, child) =>
          child match {
            case LogicalRelation(relation, _, _, _) =>
              if (relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
                val tableId = relation
                  .asInstanceOf[CarbonDatasourceHadoopRelation]
                  .carbonTable
                  .getTableId
                if (!table.getTableId.equalsIgnoreCase(tableId)) {
                  throw new AnalysisException(
                    "The query table should be same with the target table")
                }
              } else {
                throw new AnalysisException("Query is not supported on this table")
              }
            case _ =>
              throw new AnalysisException("Query is not supported")
          }
        case _ =>
          throw new AnalysisException("Query is not supported")
      }
    }

    inputPlan match {
      case Project(objectList, child) =>
        if (objectList.isEmpty || objectList.size != fields.size) {
          throw new AnalysisException(
            "the output of select query is not same with the insert columns")
        }
        child match {
          case SubqueryAlias(_, _) =>
            matchRelation(child)
          case Filter(_, child) =>
            if (hasFilter) {
              matchRelation(child)
            } else {
              throw new AnalysisException("Query is not supported")
            }
          case _ =>
            throw new AnalysisException("Query is not supported")
        }
      case _ =>
        throw new AnalysisException("Query is not supported")
    }
  }

  /**
   * if the plan contain filter, move it to the projection
   */
  def optimizePlan(plan: LogicalPlan, fields: Seq[Field]): LogicalPlan = {
    plan transform {
      case p@Project(objectList, child) =>
        // add cast
        val castObjectList = addCastToProject(fields, objectList)
        if (child.isInstanceOf[Filter]) {
          val filter = child.asInstanceOf[Filter]
          val filterObjectList = addFilterToProject(filter, castObjectList)
          Project(filterObjectList, filter.child)
        } else {
          Project(castObjectList, child)
        }
      case plan => plan
    }
  }

  private def addFilterToProject(filter: Filter, castObjectList: Seq[NamedExpression]) = {
    castObjectList.map {
      case a: Alias =>
        val cw = CaseWhen(Seq((filter.condition, a.child)), None)
        Alias(cw, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
          .asInstanceOf[NamedExpression]
      case ar =>
        val cw = CaseWhen(Seq((filter.condition, ar)), None)
        Alias(cw, UUID.randomUUID().toString)(NamedExpression.newExprId, None, None)
          .asInstanceOf[NamedExpression]
    }
  }

  private def addCastToProject(fields: Seq[Field],
      objectList: Seq[NamedExpression]) = {
    fields.zipWithIndex.map { field =>
      val needCast = field._1.dataType.isDefined &&
                     !field._1.dataType.get.equalsIgnoreCase("json") &&
                     !field._1.dataType.get.equalsIgnoreCase(
                       objectList(field._2).dataType.typeName
                     )

      if (needCast) {
        val dataType = DataTypeConverterUtil.convertToCarbonType(field._1.dataType.get)
        if (!dataType.isComplexType) {
          val sparkDataType = Util.convertCarbonToSparkDataType(dataType)
          objectList(field._2) match {
            case a: Alias =>
              val c = Cast(a.child, sparkDataType)
              Alias(c, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
                .asInstanceOf[NamedExpression]
            case ar =>
              val cw = Cast(ar, sparkDataType)
              Alias(cw, UUID.randomUUID().toString)(NamedExpression.newExprId, None, None)
                .asInstanceOf[NamedExpression]
          }
        } else {
          objectList(field._2)
        }
      } else {
        objectList(field._2)
      }
    }
  }

  def deleteTempColumnFiles(
      table: CarbonTable,
      segments: util.List[Segment],
      column: CarbonColumn,
      hadoopConf: Configuration): Unit = {
    segments.asScala.map { segment =>
      val segmentFolder =
        CarbonTablePath.getSegmentPath(table.getTablePath, segment.getSegmentNo)
      if (column.isComplex) {
        val columnFolder = VectorTablePath.getComplexFolderPath(segmentFolder, column)
        FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(columnFolder, hadoopConf))
      } else {
        val filePath = VectorTablePath.getColumnFilePath(segmentFolder, column)
        val offsetPath = VectorTablePath.getOffsetFilePath(segmentFolder, column)
        FileFactory.getCarbonFile(filePath, hadoopConf).delete()
        FileFactory.getCarbonFile(offsetPath, hadoopConf).delete()
      }
    }
  }
}

class ColumnScanRDD(
    tableInfo: TableInfo,
    column: CarbonColumn,
    partitions: Array[Partition],
    hadoopConf: Configuration,
    sc: SparkContext) extends RDD[InternalRow](sc, Nil) {

  override def getPartitions: Array[Partition] = partitions

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    new Iterator[InternalRow] {
      private var havePair = false
      private var finished = false
      private var first = true

      val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
      VectorTableInputFormat.setSegmentNo(
        inputSplit.asInstanceOf[CarbonMultiBlockSplit].getAllSplits.get(0).getSegmentId
      )
      val table = CarbonTable.buildFromTableInfo(tableInfo)
      var reader = new VectorColumnReader(table, column, getConf, false)
      val jobTrackerId: String = {
        val formatter = new SimpleDateFormat("yyyyMMddHHmm")
        formatter.format(new Date())
      }
      val taskId = split.index
      val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      val attemptContext = new TaskAttemptContextImpl(getConf, attemptId)
      val closeReader = (_: TaskContext) => {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>
              LogServiceFactory.getLogService(this.getClass.getCanonicalName).error(e)
          }
          reader = null
        }
      }

      override def hasNext: Boolean = {
        if (context.isInterrupted) {
          throw new TaskKilledException
        }
        if (first) {
          first = false
          context.addTaskCompletionListener(closeReader)
          // initialize the reader
          reader.initialize(inputSplit, attemptContext)
        }
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          havePair = !finished
        }
        if (finished) {
          closeReader.apply(context)
        }
        !finished
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val value = reader.getCurrentValue
        value.asInstanceOf[InternalRow]
      }
    }
  }

  val config = SparkSQLUtil.broadCastHadoopConf(sparkContext, hadoopConf)

  def getConf: Configuration = {
    config.value.value
  }
}
