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
package org.apache.carbondata.core.scan.executor.impl;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Executors;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.primarykey.IteratorHolder;
import org.apache.carbondata.core.scan.primarykey.IteratorRowHolder;
import org.apache.carbondata.core.scan.primarykey.IteratorVectorHolder;
import org.apache.carbondata.core.scan.primarykey.PrimaryKeyDeleteVectorDetailQueryResultIterator;
import org.apache.carbondata.core.scan.primarykey.PrimaryKeyRowComparator;
import org.apache.carbondata.core.scan.primarykey.PrimaryKeyVectorComparator;
import org.apache.carbondata.core.scan.primarykey.PrimaryKeyVectorDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.iterator.ChunkRowIterator;
import org.apache.carbondata.core.scan.result.iterator.DetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.iterator.VectorDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.util.DataTypeConverterImpl;

import org.apache.hadoop.conf.Configuration;

/**
 * Below class will be used to execute the detail query and returns columnar vectors.
 */
public class MVCCVectorDetailQueryExecutor extends AbstractQueryExecutor<Object> {

  private boolean isUpdate;

  public MVCCVectorDetailQueryExecutor(Configuration configuration, boolean isUpdate) {
    super(configuration);
    this.isUpdate = isUpdate;
  }

  @Override public CarbonIterator<Object> execute(QueryModel queryModel)
      throws QueryExecutionException, IOException {
    queryModel = queryModel.getCopy();
    queryModel.setConverter(new DataTypeConverterImpl());
    CarbonDimension versionColumn = null;
    List<CarbonDimension> primaryKeyCols = new ArrayList<>();
    for (CarbonDimension schema : queryModel.getTable().getAllDimensions()) {
      if (schema.getColumnSchema().isPrimaryKeyColumn()) {
        primaryKeyCols.add(schema);
      }
      // TODO make the version column configurable
      if (schema.getColName().equalsIgnoreCase("timestamp")) {
        versionColumn = schema;
      }
    }
    DataType[] dataTypes = new DataType[primaryKeyCols.size()];
    for (int i = 0; i < primaryKeyCols.size(); i++) {
      dataTypes[i] = primaryKeyCols.get(i).getDataType();
    }
    int[] primaryKeyOrdinals = new int[primaryKeyCols.size()];
    int[] timestampOrdinal = new int[1];

    fillPrimaryKeyOrdinals(primaryKeyCols, primaryKeyOrdinals, queryModel);
    fillPrimaryKeyOrdinals(Arrays.asList(versionColumn), timestampOrdinal, queryModel);
    int tupleIdex = -1;
    if (isUpdate) {
      CarbonDimension tupleId = queryModel.getTable()
          .getDimensionByName(queryModel.getTable().getTableName(),
              CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID);
      ProjectionDimension projectionDimension = new ProjectionDimension(tupleId);
      projectionDimension.setOrdinal(queryModel.getProjectionDimensions().size() + queryModel.getProjectionMeasures().size());
      queryModel.getProjectionDimensions().add(projectionDimension);
      tupleIdex = projectionDimension.getOrdinal();
    }
    PrimaryKeyVectorComparator comparator =
        new PrimaryKeyVectorComparator(dataTypes, primaryKeyOrdinals);
    this.setExecutorService(Executors.newCachedThreadPool());
    AbstractQueue<IteratorHolder> recordHolder = new PriorityQueue<>();
    List<BlockExecutionInfo> blockExecutionInfos = getBlockExecutionInfos(queryModel);

    for (BlockExecutionInfo executionInfo : blockExecutionInfos) {
      if (executionInfo.getDataBlock().getDataRefNode().getBlockInfo().getVersion()
          == ColumnarFormatVersion.R1) {
        executionInfo.setVectorBatchCollector(false);
        DetailQueryResultIterator iterator =
            new DetailQueryResultIterator(new ArrayList(Arrays.asList(executionInfo)), queryModel,
                queryProperties.executorService);
        List<Object[]> objects = new ArrayList<>();
        ChunkRowIterator rowIterator = new ChunkRowIterator(iterator);
        while (rowIterator.hasNext()) {
          objects.add(rowIterator.next());
        }
        Collections.sort(objects, new PrimaryKeyRowComparator(dataTypes, primaryKeyOrdinals));
        IteratorRowHolder holder =
            new IteratorRowHolder(comparator, objects.iterator(), executionInfo);
        if (holder.hasNext()) {
          holder.read();
          recordHolder.add(holder);
        }
      } else {
        if (isUpdate) {
          executionInfo.setDirectVectorFill(false);
        }
        VectorDetailQueryResultIterator iterator =
            new VectorDetailQueryResultIterator(new ArrayList(Arrays.asList(executionInfo)),
                queryModel, queryProperties.executorService);
        IteratorVectorHolder holder =
            new IteratorVectorHolder(comparator, iterator, createColumnarBatch(queryModel),
                executionInfo);
        if (holder.hasNext()) {
          holder.read();
          recordHolder.add(holder);
        }
      }
    }

    int dataLength =
        queryModel.getProjectionDimensions().size() + queryModel.getProjectionMeasures().size();
    if (isUpdate) {
      this.queryIterator = new PrimaryKeyDeleteVectorDetailQueryResultIterator(recordHolder,
          new PrimaryKeyRowComparator(dataTypes, primaryKeyOrdinals), dataLength,
          timestampOrdinal[0], tupleIdex, queryModel.getUpdateTimeStamp());
    } else {
      this.queryIterator = new PrimaryKeyVectorDetailQueryResultIterator(recordHolder,
          new PrimaryKeyRowComparator(dataTypes, primaryKeyOrdinals), dataLength,
          timestampOrdinal[0]);
    }
    return this.queryIterator;
  }


  private void fillPrimaryKeyOrdinals(List<CarbonDimension> primaryKeyCols,
      int[] primaryKeyOrdinals, QueryModel queryModel) {
    List<ProjectionDimension> projectionDimensions = queryModel.getProjectionDimensions();

    int k = 0;
    for (CarbonDimension keyCol : primaryKeyCols) {
      boolean found = false;
      for (int j = 0; j < projectionDimensions.size(); j++) {
        if (projectionDimensions.get(j).getDimension().equals(keyCol)) {
          primaryKeyOrdinals[k++] = projectionDimensions.get(j).getOrdinal();
          found = true;
          break;
        }
      }
      if (!found) {
        ProjectionDimension projectionDimension = new ProjectionDimension(keyCol);
        projectionDimensions.add(projectionDimension);
        projectionDimension.setOrdinal(
            projectionDimensions.size() + queryModel.getProjectionMeasures().size() - 1);
        primaryKeyOrdinals[k++] = projectionDimension.getOrdinal();
      }
    }
  }

  private CarbonColumnarBatch createColumnarBatch(QueryModel queryModel) {
    List<ProjectionDimension> queryDimension = queryModel.getProjectionDimensions();
    List<ProjectionMeasure> queryMeasures = queryModel.getProjectionMeasures();
    StructField[] fields = new StructField[queryDimension.size() + queryMeasures.size()];
    for (ProjectionDimension dim : queryDimension) {
      fields[dim.getOrdinal()] =
          new StructField(dim.getColumnName(), dim.getDimension().getDataType());
    }
    for (ProjectionMeasure msr : queryMeasures) {
      DataType dataType = msr.getMeasure().getDataType();
      if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.SHORT || dataType == DataTypes.INT
          || dataType == DataTypes.LONG || dataType == DataTypes.FLOAT
          || dataType == DataTypes.BYTE) {
        fields[msr.getOrdinal()] =
            new StructField(msr.getColumnName(), msr.getMeasure().getDataType());
      } else if (DataTypes.isDecimal(dataType)) {
        fields[msr.getOrdinal()] = new StructField(msr.getColumnName(), DataTypes
            .createDecimalType(msr.getMeasure().getPrecision(), msr.getMeasure().getScale()));
      } else {
        fields[msr.getOrdinal()] = new StructField(msr.getColumnName(), DataTypes.DOUBLE);
      }
    }
    CarbonColumnVector[] vectors = new CarbonColumnVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      vectors[i] = new CarbonColumnVectorImpl(
          CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT,
          fields[i].getDataType());
    }
    return new CarbonColumnarBatch(vectors,
        CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT,
        new boolean[CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT]);
  }

  public CarbonIterator getCarbonIterator() {
    return queryIterator;
  }

}
