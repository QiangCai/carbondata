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

package org.apache.carbondata.vector.file.writer.impl;


import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.vector.file.writer.ArrayWriter;
import org.apache.carbondata.vector.file.writer.ArrayWriterFactory;
import org.apache.carbondata.vector.table.VectorTablePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

/**
 * writer for sparse struct array
 */
public class SparseStructsWriter extends SparseWriter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SparseStructsWriter.class.getCanonicalName());

  private CarbonDimension dimension;
  private ArrayWriter[] childWriters;
  private Object[] defaultValues;
  private int numColumns;

  public SparseStructsWriter(CarbonTable table, CarbonColumn column) {
    super(table, column);
  }

  @Override
  public void open(String outputFolder, Configuration hadoopConf) throws IOException {
    String columnFolder = VectorTablePath.getComplexFolderPath(outputFolder, column);
    FileFactory.mkdirs(columnFolder, hadoopConf);
    String offsetFilePath = VectorTablePath.getOffsetFilePath(columnFolder, column);
    offsetOutput =
        FileFactory.getDataOutputStream(offsetFilePath, FileFactory.getFileType(offsetFilePath));
    dimension = (CarbonDimension) column;
    // init child writers
    List<CarbonDimension> childDimensions = dimension.getListOfChildDimensions();
    numColumns = childDimensions.size();
    childWriters = new ArrayWriter[numColumns];
    for (int index = 0; index < numColumns; index++) {
      childWriters[index] = ArrayWriterFactory.getArrayWriter(table, childDimensions.get(index));
      childWriters[index].open(columnFolder, hadoopConf);
    }
    // init default value
    defaultValues = new Object[numColumns];
    for (int index = 0; index < numColumns; index++) {
      CarbonDimension dimension = childDimensions.get(index);
        defaultValues[index] =
            DataTypeUtil.getDataBasedOnDataType(dimension.getDefaultValue(), dimension);
    }
  }

  @Override
  public void appendObject(Object value) throws IOException {
    if (value == null) {
      offsetOutput.writeLong(offset ^ Long.MIN_VALUE);
    } else {
      Row row = ((Row) value);
      int length = numColumns <= row.size() ? numColumns : row.size();
      int index = 0;
      for (; index < length; index++) {
        childWriters[index].appendObject(row.get(index));
      }
      for (; index < numColumns; index++) {
        childWriters[index].appendObject(defaultValues[index]);
      }
      offset += 1;
      offsetOutput.writeLong(offset);
    }
  }

  @Override
  public void close() throws IOException {
    IOException ex = null;
    try {
      super.close();
    } catch (IOException e) {
      ex = e;
    }
    if (childWriters != null) {
      for (int index = 0; index < numColumns; index++) {
        try {
          childWriters[index].close();
          childWriters[index] = null;
        } catch (IOException e) {
          ex = e;
        }
      }
    }
    childWriters = null;
    if (ex != null) {
      throw ex;
    }
  }
}
