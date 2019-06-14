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

package org.apache.carbondata.vector.file.reader.impl;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.vector.file.reader.ArrayReader;
import org.apache.carbondata.vector.file.vector.ArrayVector;
import org.apache.carbondata.vector.file.vector.impl.SparseVector;
import org.apache.carbondata.vector.table.VectorTablePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * read sparse array data file
 */
@InterfaceAudience.Internal
@InterfaceStability.Evolving
public abstract class SparseReader implements ArrayReader {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SparseReader.class.getCanonicalName());

  protected final CarbonTable table;
  protected final CarbonColumn column;
  protected DataInputStream dataInput;
  protected DataInputStream offsetInput;

  public SparseReader(CarbonTable table, CarbonColumn column) {
    this.table = table;
    this.column = column;
  }

  @Override
  public void open(String inputFolder, Configuration hadoopConf) throws IOException {
    String columnFilePath = VectorTablePath.getColumnFilePath(inputFolder, column);
    dataInput =
        FileFactory.getDataInputStream(columnFilePath, FileFactory.getFileType(columnFilePath));
    String offsetFilePath = VectorTablePath.getOffsetFilePath(inputFolder, column);
    offsetInput =
        FileFactory.getDataInputStream(offsetFilePath, FileFactory.getFileType(columnFilePath));
  }

  /**
   * read data file and fill data into vector
   * @param vector
   * @param rowCount
   * @return read row count, -1 if end
   * @throws IOException
   */
  @Override
  public int read(ArrayVector vector, int rowCount) throws IOException {
    return ((SparseVector) vector).fillVector(offsetInput, dataInput, rowCount);
  }

  @Override
  public void close() throws IOException {
    IOException ex = null;
    try {
      offsetInput.close();
    } catch (IOException e) {
      LOGGER.error("Failed to close offset input stream", e);
      ex = e;
    }
    try {
      dataInput.close();
    } catch (IOException e) {
      LOGGER.error("Failed to close data input stream", e);
      ex = e;
    }
    if (ex != null) {
      throw ex;
    }
  }
}
