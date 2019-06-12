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

package org.apache.carbondata.vector.file.writer;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.vector.file.writer.impl.SparseArraysWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseBinaryWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseBooleansWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseDatesWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseDecimalsWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseDoublesWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseFloatsWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseIntsWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseLongsWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseMapWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseShortsWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseStringsWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseStructsWriter;
import org.apache.carbondata.vector.file.writer.impl.SparseTimestampsWriter;

import org.apache.log4j.Logger;

/**
 * factory to use array writer
 */
public class ArrayWriterFactory {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ArrayWriterFactory.class.getCanonicalName());

  /**
   * create array writer for a column
   * @param column
   * @return
   */
  public static ArrayWriter getArrayWriter(CarbonTable table, CarbonColumn column) {
    int id = column.getDataType().getId();
    if (id == DataTypes.STRING.getId()) {
      return new SparseStringsWriter(table, column);
    } else if (id == DataTypes.DATE.getId()) {
      return new SparseDatesWriter(table, column);
    } else if (id == DataTypes.TIMESTAMP.getId()) {
      return new SparseTimestampsWriter(table, column);
    } else if (id == DataTypes.BOOLEAN.getId()) {
      return new SparseBooleansWriter(table, column);
    } else if (id == DataTypes.SHORT.getId()) {
      return new SparseShortsWriter(table, column);
    } else if (id == DataTypes.INT.getId()) {
      return new SparseIntsWriter(table, column);
    } else if (id == DataTypes.LONG.getId()) {
      return new SparseLongsWriter(table, column);
    } else if (id == DataTypes.FLOAT.getId()) {
      return new SparseFloatsWriter(table, column);
    } else if (id == DataTypes.DOUBLE.getId()) {
      return new SparseDoublesWriter(table, column);
    } else if (id == DataTypes.DECIMAL_TYPE_ID) {
      return new SparseDecimalsWriter(table, column);
    } else if (id == DataTypes.BINARY.getId()) {
      return new SparseBinaryWriter(table, column);
    } else if (id == DataTypes.ARRAY_TYPE_ID) {
      return new SparseArraysWriter(table, column);
    } else if (id == DataTypes.STRUCT_TYPE_ID) {
      return new SparseStructsWriter(table, column);
    } else if (id == DataTypes.MAP_TYPE_ID) {
      return new SparseMapWriter(table, column);
    } else if (id == DataTypes.VARCHAR.getId()) {
      return new SparseStringsWriter(table, column);
    } else {
      throw new RuntimeException(
          "vector table not support data type: " + column.getDataType().getName());
    }
  }

}
