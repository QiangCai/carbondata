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

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.vector.file.writer.ArrayWriter;
import org.apache.carbondata.vector.file.writer.ArrayWriterFactory;
import org.apache.carbondata.vector.table.VectorTablePath;

import org.apache.hadoop.conf.Configuration;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

/**
 * writer for sparse array array ^_^...
 */
public class SparseArraysWriter extends SparseWriter {

  private ArrayWriter childWriter;

  public SparseArraysWriter(CarbonTable table, CarbonColumn column) {
    super(table, column);
  }

  @Override public void open(String outputFolder, Configuration hadoopConf) throws IOException {
    String columnFolder = VectorTablePath.getComplexFolderPath(outputFolder, column);
    FileFactory.mkdirs(columnFolder, hadoopConf);
    String offsetFilePath = VectorTablePath.getOffsetFilePath(columnFolder, column);
    offsetOutput =
        FileFactory.getDataOutputStream(offsetFilePath, FileFactory.getFileType(offsetFilePath));

    // init child writers
    CarbonDimension childDimension = ((CarbonDimension) column).getListOfChildDimensions().get(0);
    childWriter = ArrayWriterFactory.createArrayWriter(table, childDimension);
    childWriter.open(columnFolder, hadoopConf);
  }

  @Override public void appendObject(Object value) throws IOException {
    if (value == null) {
      offsetOutput.writeLong(offset ^ Long.MIN_VALUE);
    } else {
      WrappedArray.ofRef seq = ((WrappedArray.ofRef) value);
      Iterator iterator = seq.iterator();
      while (iterator.hasNext()) {
        childWriter.appendObject(iterator.next());
      }
      offset += seq.size();
      offsetOutput.writeLong(offset);
    }
  }

  @Override
  public void close() throws IOException {
    IOException ex = ArrayWriterFactory.destroyArrayWriter(
        "Failed to close child writer of array writer",
        childWriter);
    childWriter = null;
    try {
      super.close();
    } catch (IOException e) {
      ex = e;
    }
    if (ex != null) {
      throw ex;
    }
  }
}
