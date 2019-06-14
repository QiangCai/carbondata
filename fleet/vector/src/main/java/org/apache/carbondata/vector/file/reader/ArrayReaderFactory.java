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

package org.apache.carbondata.vector.file.reader;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.vector.file.reader.impl.SparseArraysReader;
import org.apache.carbondata.vector.file.reader.impl.SparseMapsReader;
import org.apache.carbondata.vector.file.reader.impl.SparsePrimitiveReader;
import org.apache.carbondata.vector.file.reader.impl.SparseStructsReader;

import org.apache.log4j.Logger;

/**
 * factory to use array reader
 */
public class ArrayReaderFactory {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ArrayReaderFactory.class.getCanonicalName());

  public static ArrayReader getArrayReader(CarbonTable table, CarbonColumn column) {
    int id = column.getDataType().getId();
    if (id == DataTypes.STRING.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.DATE.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.TIMESTAMP.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.BOOLEAN.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.SHORT.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.INT.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.LONG.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.FLOAT.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.DOUBLE.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.DECIMAL_TYPE_ID) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.BINARY.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else if (id == DataTypes.ARRAY_TYPE_ID) {
      return new SparseArraysReader(table, column);
    } else if (id == DataTypes.STRUCT_TYPE_ID) {
      return new SparseStructsReader(table, column);
    } else if (id == DataTypes.MAP_TYPE_ID) {
      return new SparseMapsReader(table, column);
    } else if (id == DataTypes.VARCHAR.getId()) {
      return new SparsePrimitiveReader(table, column);
    } else {
      throw new RuntimeException(
          "vector table not support data type: " + column.getDataType().getName());
    }
  }

}
