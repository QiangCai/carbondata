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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.vector.file.writer.ArrayWriter;
import org.apache.carbondata.vector.table.VectorTablePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * write sparse array data to file.
 * sparse array data means it exists null value.
 */
public abstract class SparseWriter implements ArrayWriter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SparseWriter.class.getCanonicalName());

  protected final CarbonTable table;
  protected final CarbonColumn column;
  protected DataOutputStream dataOutput;
  protected DataOutputStream offsetOutput;
  protected long offset = 0;

  SparseWriter(final CarbonTable table, final CarbonColumn column) {
    this.table = table;
    this.column = column;
  }

  @Override
  public void open(final String folderPath, final Configuration configuration) throws IOException {
    String columnFilePath = VectorTablePath.getColumnFilePath(folderPath, column);
    dataOutput =
        FileFactory.getDataOutputStream(columnFilePath, FileFactory.getFileType(columnFilePath));
    String offsetFilePath = VectorTablePath.getOffsetFilePath(folderPath, column);
    offsetOutput =
        FileFactory.getDataOutputStream(offsetFilePath, FileFactory.getFileType(offsetFilePath));
  }

  @Override
  public void appendObject(final Object value) throws IOException {
    if (value == null) {
      offsetOutput.writeLong(offset ^ Long.MIN_VALUE);
    } else {
      byte[] bytes = toBytes(value);
      dataOutput.write(bytes, 0, bytes.length);
      offset += bytes.length;
      offsetOutput.writeLong(offset);
    }
  }

  /**
   * convert a not null value to byte array
   * @param value
   * @throws IOException
   */
  protected byte[] toBytes(Object value) {
    return new byte[0];
  }

  @Override
  public void close() throws IOException {
    IOException ex = null;
    if (dataOutput != null) {
      try {
        dataOutput.close();
      } catch (IOException e) {
        ex = e;
        LOGGER.error("Failed to close data output stream", e);
      }
    }
    if (offsetOutput != null) {
      try {
        offsetOutput.close();
      } catch (IOException e) {
        ex = e;
        LOGGER.error("Failed to close offset output stream", e);
      }
    }
    if (ex != null) {
      throw ex;
    }
  }
}
