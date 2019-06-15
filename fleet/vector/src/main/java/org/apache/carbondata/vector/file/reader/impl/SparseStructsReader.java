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

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.vector.file.reader.ArrayReader;
import org.apache.carbondata.vector.file.reader.ArrayReaderFactory;
import org.apache.carbondata.vector.table.VectorTablePath;

import org.apache.hadoop.conf.Configuration;

/**
 * reader for primitive data type array
 */
public class SparseStructsReader extends SparseReader {

  private ArrayReader[] childReaders;
  private int numColumns;

  public SparseStructsReader(CarbonTable table, CarbonColumn column) {
    super(table, column);
  }

  @Override
  public void open(String inputFolder, Configuration hadoopConf) throws IOException {
    String columnFolder = VectorTablePath.getComplexFolderPath(inputFolder, column);
    String offsetFilePath = VectorTablePath.getOffsetFilePath(columnFolder, column);
    offsetInput =
        FileFactory.getDataInputStream(offsetFilePath, FileFactory.getFileType(offsetFilePath));
    // init child readers
    List<CarbonDimension> childDimensions = ((CarbonDimension) column).getListOfChildDimensions();
    numColumns = childDimensions.size();
    childReaders = new ArrayReader[numColumns];
    for (int index = 0; index < numColumns; index++) {
      childReaders[index] = ArrayReaderFactory.createArrayReader(table, childDimensions.get(0));
      childReaders[index].open(columnFolder, hadoopConf);
    }
  }

  public ArrayReader[] getChildReaders() {
    return childReaders;
  }

  @Override
  public void close() throws IOException {
    IOException ex = ArrayReaderFactory.destroyArrayReader(
        "Failed to close child readers of struct reader", childReaders);
    if (childReaders != null) {
      for (int index = 0; index < numColumns; index++) {
        childReaders[index] = null;
      }
      childReaders = null;
    }
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
