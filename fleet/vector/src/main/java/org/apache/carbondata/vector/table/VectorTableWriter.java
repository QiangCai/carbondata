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

package org.apache.carbondata.vector.table;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.vector.exception.VectorTableException;
import org.apache.carbondata.vector.file.writer.ArrayWriter;
import org.apache.carbondata.vector.file.writer.ArrayWriterFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * writer API for vector table
 */
@InterfaceAudience.User
@InterfaceStability.Stable
public class VectorTableWriter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(VectorTableWriter.class.getCanonicalName());

  private final Configuration hadoopConf;
  private final CarbonTable table;
  private final String segmentPath;
  private ArrayWriter[] arrayWriters;
  private Object[] defaultValues;
  private int numColumns;
  private boolean isFirstRow = true;

  public VectorTableWriter(final CarbonLoadModel loadModel, final Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
    table = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    segmentPath = CarbonTablePath.getSegmentPath(table.getTablePath(), loadModel.getSegmentId());
  }

  /**
   * init writer and make directory for columns
   */
  private synchronized void initWriter() throws VectorTableException {
    if (isFirstRow) {
      List<CarbonColumn> columns = table.getCreateOrderColumn(table.getTableName());
      numColumns = columns.size();
      // init writer
      arrayWriters = new ArrayWriter[numColumns];
      try {
        for (int index = 0; index < numColumns; index++) {
          arrayWriters[index] = ArrayWriterFactory.getArrayWriter(table, columns.get(index));
          arrayWriters[index].open(segmentPath, hadoopConf);
        }
      } catch (IOException e) {
        String message = "Failed to init array writer";
        LOGGER.error(message, e);
        throw new VectorTableException(message, e);
      }
      // init default value
      defaultValues = new Object[numColumns];
      for (int index = 0; index < numColumns; index++) {
        CarbonColumn column = columns.get(index);
        if (columns.get(index).isDimension()) {
          defaultValues[index] = DataTypeUtil
              .getDataBasedOnDataType(column.getDefaultValue(), (CarbonDimension) column);
        } else {
          defaultValues[index] = RestructureUtil
              .getMeasureDefaultValue(column.getColumnSchema(), column.getDefaultValue());
        }
      }
    }
    isFirstRow = false;
  }

  /**
   * write one row
   */
  public void write(Object[] row) throws VectorTableException {
    if (isFirstRow) {
      initWriter();
    }

    int length = 0;
    if (row != null) {
      length = numColumns <= row.length ? numColumns : row.length;
    }
    int index = 0;
    try {
      for (; index < length; index++) {
        arrayWriters[index].appendObject(row[index]);
      }
      for (; index < numColumns; index++) {
        arrayWriters[index].appendObject(defaultValues[index]);
      }
    } catch (IOException e) {
      String message = "Failed to write row";
      LOGGER.error(message, e);
      throw new VectorTableException(message, e);
    }
  }

  /**
   * finally release resource
   */
  public void close() {
    if (arrayWriters != null) {
      for (ArrayWriter arrayWriter : arrayWriters) {
        try {
          if (arrayWriter != null) {
            arrayWriter.close();
          }
        } catch (Exception e) {
          // catch exception to continue close next writer
          LOGGER.error("Failed to close array file writer", e);
        }
      }
    }
  }

}
