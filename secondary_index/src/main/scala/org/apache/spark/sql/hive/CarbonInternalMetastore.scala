/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.spark.sql.hive

import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.ManageDictionaryAndBTree
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.core.metadata.IndexMetadata
import org.apache.carbondata.spark.spark.indextable.{IndexTableInfo, IndexTableUtil}

object CarbonInternalMetastore {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * This method can be used to delete the index table and update the parent table and hive
   * metadata.
   *
   * @param indexCarbonTable
   * @param tableStorePath
   * @param parentCarbonTable
   * @param removeEntryFromParentTable if true then the index table info would be removed from
   *                                   the Parent table
   * @param sparkSession
   */
  def dropIndexTable(indexTableIdentifier: TableIdentifier, indexCarbonTable: CarbonTable,
      tableStorePath: String,
      parentCarbonTable: CarbonTable,
      removeEntryFromParentTable: Boolean)(sparkSession: SparkSession) {
    val dbName = indexTableIdentifier.database.get
    val tableName = indexTableIdentifier.table
    try {
      if (indexCarbonTable != null) {
        ManageDictionaryAndBTree.clearBTreeAndDictionaryLRUCache(indexCarbonTable)
        LOGGER.info(s"Deleting index table $dbName.$tableName")
        CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .dropTable(indexCarbonTable.getAbsoluteTableIdentifier)(sparkSession)
        if (removeEntryFromParentTable && parentCarbonTable != null) {
          val parentTableName = parentCarbonTable.getTableName
          val relation: LogicalPlan = CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .lookupRelation(Some(dbName), parentTableName)(sparkSession)
          val indexInfo = if (relation != null) {
            CarbonInternalScalaUtil
              .getIndexInfo(relation.asInstanceOf[CarbonRelation].metaData.carbonTable)
          } else {
            sys.error(s"Table $dbName.$parentTableName does not exists")
          }
          sparkSession.sessionState.catalog.dropTable(indexTableIdentifier, true, false)
          // even if folders are deleted from carbon store it can happen that table exists in hive
          CarbonInternalHiveMetadataUtil
            .invalidateAndUpdateIndexInfo(indexTableIdentifier, indexInfo, parentCarbonTable)(
              sparkSession)
          // clear parent table from meta store cache as it is also required to be
          // refreshed when SI table is dropped
          DataMapStoreManager.getInstance()
            .clearDataMaps(indexCarbonTable.getAbsoluteTableIdentifier)
          removeTableFromMetadataCache(dbName, indexCarbonTable.getTableName)(sparkSession)
          removeTableFromMetadataCache(dbName, parentTableName)(sparkSession)
        }
      }
    } finally {
      // Even if some exception occurs we will try to remove the table from catalog to avoid
      // stale state.
      sparkSession.sessionState.catalog
        .dropTable(indexTableIdentifier, ignoreIfNotExists = true, purge = false)
      sparkSession.sessionState.catalog.refreshTable(indexTableIdentifier)
      LOGGER.info(s"Deleted index table $dbName.$tableName")
    }
  }

  def removeTableFromMetadataCache(dbName: String, tableName: String)
    (sparkSession: SparkSession): Unit = {
    CarbonEnv.getInstance(sparkSession).carbonMetaStore.removeTableFromMetadata(dbName, tableName)
  }

  /**
   * This method will delete the index tables silently. We want this because even if one index
   * delete fails, we need to try delete on all other index tables as well.
   *
   * @param carbonTableIdentifier
   * @param storePath
   * @param sparkSession
   */
  def deleteIndexSilent(carbonTableIdentifier: TableIdentifier,
      storePath: String,
      parentCarbonTable: CarbonTable)(sparkSession: SparkSession): Unit = {
    val dbName = carbonTableIdentifier.database
    val indexTable = carbonTableIdentifier.table
    var indexCarbonTable: CarbonTable = null
    try {
      indexCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        .lookupRelation(dbName, indexTable)(sparkSession)
        .asInstanceOf[CarbonRelation].carbonTable
    } catch {
      case e: Exception =>
        LOGGER.error("Exception occurred while drop index table for : " +
                     s"$dbName.$indexTable : ${ e.getMessage }")
    }
    finally {
      try {
        dropIndexTable(carbonTableIdentifier,
          indexCarbonTable,
          storePath,
          parentCarbonTable,
          removeEntryFromParentTable = true
        )(sparkSession)
      } catch {
        case e: Exception =>
          LOGGER.error("Exception occurred while drop index table for : " +
                       s"$dbName.$indexTable : ${ e.getMessage }")
      }
    }
  }

  def refreshIndexInfo(dbName: String, tableName: String,
      carbonTable: CarbonTable, needLock: Boolean = true)(sparkSession: SparkSession): Unit = {
    val indexTableExists = CarbonInternalScalaUtil.isIndexTableExists(carbonTable)
    // tables created without property "indexTableExists", will return null, for those tables enter
    // into below block, gather the actual data from hive and then set this property to true/false
    // then once the property has a value true/false, make decision based on the property value
    if (null != carbonTable && (null == indexTableExists || indexTableExists.toBoolean)) {
      // When Index information is not loaded in main table, then it will fetch
      // index info from hivemetastore and set it in the carbon table.
      val indexTableMap = new ConcurrentHashMap[String, java.util.List[String]]
      try {
        val (isIndexTable, parentTableName, indexInfo, parentTablePath, parentTableId, schema) =
          indexInfoFromHive(dbName, tableName)(sparkSession)
        if (isIndexTable.equals("true")) {
          val indexMeta = new IndexMetadata(indexTableMap,
            parentTableName,
            true,
            parentTablePath,
            parentTableId)
          setDictionaryPathInCarbonTable(carbonTable, indexMeta)
          carbonTable.getTableInfo.getFactTable.getTableProperties
            .put(carbonTable.getCarbonTableIdentifier.getTableId, indexMeta.serialize)
        } else {
          IndexTableUtil.fromGson(indexInfo)
            .foreach { indexTableInfo =>
              indexTableMap
                .put(indexTableInfo.getTableName, indexTableInfo.getIndexCols)
            }
          val indexMetadata = new IndexMetadata(indexTableMap,
              parentTableName,
              isIndexTable.toBoolean,
              parentTablePath, parentTableId)
          carbonTable.getTableInfo.getFactTable.getTableProperties
            .put(carbonTable.getCarbonTableIdentifier.getTableId, indexMetadata.serialize)
        }
        if (null == indexTableExists && !isIndexTable.equals("true")) {
          val indexTables = CarbonInternalScalaUtil.getIndexesTables(carbonTable)
          val tableIdentifier = new TableIdentifier(carbonTable.getTableName,
            Some(carbonTable.getDatabaseName))
          if (indexTables.isEmpty) {
            // modify the tableProperties of mainTable by adding "indexTableExists" property
            // to false as there is no index table for this table
            CarbonInternalScalaUtil
              .addOrModifyTableProperty(carbonTable,
                Map("indexTableExists" -> "false"), schema, needLock)(sparkSession)
          } else {
            // modify the tableProperties of mainTable by adding "indexTableExists" property
            // to true as there are some index table for this table
            CarbonInternalScalaUtil
              .addOrModifyTableProperty(carbonTable,
                Map("indexTableExists" -> "true"), schema, needLock)(sparkSession)
          }
        }
      } catch {
        case e: Exception =>
          // In case of creating a table, hivetable will not be available.
          LOGGER.error(e.getMessage, e)
      }
    }
  }

  /**
   * set dictionary location in carbon table
   *
   * @param carbonTable
   * @param indexMetadata
   */
  private def setDictionaryPathInCarbonTable(carbonTable: CarbonTable,
      indexMetadata: IndexMetadata): Unit = {
    val parentTableAbsoluteTableIdentifier = AbsoluteTableIdentifier
      .from(indexMetadata.getParentTablePath,
        carbonTable.getDatabaseName,
        indexMetadata.getParentTableName,
        indexMetadata.getParentTableId)
    val parentCarbonTablePath = parentTableAbsoluteTableIdentifier.getTablePath
    carbonTable.getTableInfo.getFactTable.getTableProperties
      .put(CarbonCommonConstants.DICTIONARY_PATH,
        CarbonTablePath.getMetadataPath(parentCarbonTablePath))
  }

  private def indexInfoFromHive(databaseName: String, tableName: String)
    (sparkSession: SparkSession): (String, String, String, String, String, String) = {
    val hiveTable = sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier(tableName, Some(databaseName)))
    val indexList = hiveTable.storage.properties.getOrElse(
      "indexInfo", IndexTableUtil.toGson(new Array[IndexTableInfo](0)))

    val datasourceOptions = optionsValueFromParts(hiveTable)

    val isIndexTable = datasourceOptions.getOrElse("isIndexTable", "false")
    val parentTableName = datasourceOptions.getOrElse("parentTableName", "")
    val parentTablePath = if (!parentTableName.isEmpty) {
      CarbonEnv
        .getCarbonTable(TableIdentifier(parentTableName, Some(databaseName)))(sparkSession)
        .getTablePath
    } else {
      ""
    }
    val parentTableId = datasourceOptions.getOrElse("parentTableId", "")
    (isIndexTable, parentTableName, indexList, parentTablePath, parentTableId, hiveTable.schema
      .json)
  }

  private def optionsValueFromParts(table: CatalogTable): Map[String, String] = {
    val optionsCombined = new HashMap[String, String]
    val optionsKeys: Option[String] =
      table.storage.properties.get("spark.sql.sources.options.keys.numParts").map { numParts =>
        combinePartsFromSerdeProps(numParts, "spark.sql.sources.options.keys", table).mkString
      }
    optionsKeys match {
      case Some(optKeys) =>
        optKeys.split(",").foreach { optKey =>
          table.storage.properties.get(s"$optKey.numParts").map { numParts =>
            optionsCombined.put(optKey,
              combinePartsFromSerdeProps(numParts, optKey, table).mkString)
          }
        }
        optionsCombined.asScala.toMap
      case None =>
        LOGGER.info(s"spark.sql.sources.options.keys expected, but read nothing")
        table.storage.properties
    }
  }

  private def combinePartsFromSerdeProps(numParts: String,
      key: String, table: CatalogTable): Seq[String] = {
    val keysParts = (0 until numParts.toInt).map { index =>
      val keysPart =
        table.storage.properties.get(s"$key.part.$index").orNull
      if (keysPart == null) {
        throw new AnalysisException(
          s"Could not read $key from the metastore because it is corrupted " +
          s"(missing part $index of the $key, $numParts parts are expected).")
      }
      keysPart
    }
    keysParts
  }

  def deleteTableDirectory(dbName: String, tableName: String,
    sparkSession: SparkSession): Unit = {
    val databaseLocation = CarbonEnv.getDatabaseLocation(dbName, sparkSession)
    val tablePath = databaseLocation + CarbonCommonConstants.FILE_SEPARATOR + tableName.toLowerCase
    val metadataFilePath =
      CarbonTablePath.getMetadataPath(tablePath)
    val fileType = FileFactory.getFileType(metadataFilePath)
    if (FileFactory.isFileExist(metadataFilePath, fileType)) {
      val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
      CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
    }
  }
}
