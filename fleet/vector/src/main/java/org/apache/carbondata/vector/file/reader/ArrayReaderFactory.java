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

import java.io.DataInputStream;
import java.io.IOException;

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

  /**
   * create ArrayReader for a column
   * @param table
   * @param column
   * @return
   */
  public static ArrayReader createArrayReader(CarbonTable table, CarbonColumn column) {
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
      String message = String.format(
          "vector table %s column %s not support reader data type: %s",
          table.getTableUniqueName(),
          column.getColName());
      LOGGER.error(message);
      throw new RuntimeException(message);
    }
  }

  /**
   * destroy ArrayReader to release reader resource
   * @param errorMessage
   * @param readers
   * @return
   */
  public static IOException destroyArrayReader(String errorMessage, ArrayReader... readers) {
    if (readers == null) {
      return null;
    }
    IOException ex = null;
    for (ArrayReader reader : readers) {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          if (errorMessage == null) {
            LOGGER.error(e);
          } else {
            LOGGER.error(errorMessage, e);
          }
          ex = e;
        }
      }
    }
    return ex;
  }

  /**
   * destroy InputStream to release input resource
   * @param errorMessage
   * @param inputs
   * @return
   */
  public static IOException destroyInputStream(String errorMessage, DataInputStream... inputs) {
    if (inputs == null) {
      return null;
    }
    IOException ex = null;
    for (DataInputStream input : inputs) {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          if (errorMessage == null) {
            LOGGER.error(e);
          } else {
            LOGGER.error(errorMessage, e);
          }
          ex = e;
        }
      }
    }
    return ex;
  }
}
