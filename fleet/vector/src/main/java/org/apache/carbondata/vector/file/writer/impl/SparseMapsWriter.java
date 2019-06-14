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

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.vector.file.writer.ArrayWriter;
import org.apache.carbondata.vector.file.writer.ArrayWriterFactory;
import org.apache.carbondata.vector.table.VectorTablePath;

import org.apache.hadoop.conf.Configuration;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Map;

/**
 * writer for sparse map array
 */
public class SparseMapsWriter extends SparseWriter {

  private CarbonDimension dimension;
  private ArrayWriter keyWriter;
  private ArrayWriter valueWriter;

  public SparseMapsWriter(CarbonTable table, CarbonColumn column) {
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
    List<CarbonDimension> childDimensions =
        dimension.getListOfChildDimensions().get(0).getListOfChildDimensions();
    keyWriter = ArrayWriterFactory.getArrayWriter(table, childDimensions.get(0));
    keyWriter.open(columnFolder, hadoopConf);
    valueWriter = ArrayWriterFactory.getArrayWriter(table, childDimensions.get(1));
    valueWriter.open(columnFolder, hadoopConf);
  }

  @Override
  public void appendObject(Object value) throws IOException {
    if (value == null) {
      offsetOutput.writeLong(offset ^ Long.MIN_VALUE);
    } else {
      Map map = ((Map) value);
      Iterator iterator = map.iterator();
      while (iterator.hasNext()) {
        Tuple2 tuple2 = (Tuple2) iterator.next();
        keyWriter.appendObject(tuple2._1);
        keyWriter.appendObject(tuple2._2);
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
    if (keyWriter != null) {
      try {
        keyWriter.close();
        keyWriter = null;
      } catch (IOException e) {
        ex = e;
      }
    }
    if (valueWriter != null) {
      try {
        valueWriter.close();
        keyWriter = null;
      } catch (IOException e) {
        ex = e;
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

}
