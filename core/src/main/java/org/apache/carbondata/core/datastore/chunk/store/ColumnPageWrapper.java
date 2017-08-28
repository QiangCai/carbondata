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

package org.apache.carbondata.core.datastore.chunk.store;

import java.util.BitSet;

import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * ColumnPage wrapper for dimension column reader
 */
public class ColumnPageWrapper implements DimensionColumnDataChunk {

  private ColumnPage columnPage;
  private int columnValueSize;

  public ColumnPageWrapper(ColumnPage columnPage, int columnValueSize) {
    this.columnPage = columnPage;
    this.columnValueSize = columnValueSize;
  }

  @Override
  public int fillChunkData(byte[] data, int offset, int columnIndex,
      KeyStructureInfo restructuringInfo) {
    //columnPage.fillRow(columnIndex, data, offset);
    return columnValueSize;
  }

  @Override
  public int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
      KeyStructureInfo restructuringInfo) {
    row[columnIndex] = columnPage.getInt(rowId);
    return columnIndex + 1;
  }

  @Override
  public int fillConvertedChunkData(ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    // fill the vector with data in column page
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offsetRowId = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int maxRowId = offsetRowId + columnVectorInfo.size;
    switch (columnPage.getDataType()) {
      case INT:
        DirectDictionaryGenerator generator = columnVectorInfo.directDictionaryGenerator;
        assert (generator != null);
        DataType dataType = generator.getReturnType();
        assert (dataType == DataType.INT || dataType == DataType.LONG);
        for (int rowId = offsetRowId; rowId < maxRowId; rowId++) {
          int surrogate = columnPage.getInt(rowId);
          Object valueFromSurrogate = generator.getValueFromSurrogate(surrogate);
          if (valueFromSurrogate == null) {
            vector.putNull(vectorOffset++);
          } else {
            if (dataType == DataType.INT) {
              vector.putInt(vectorOffset++, (int) valueFromSurrogate);
            } else {
              vector.putLong(vectorOffset++, (long) valueFromSurrogate);
            }
          }
        }
        break;
      case STRING:
        BitSet nullBitset = columnPage.getNullBits();
        for (int rowId = offsetRowId; rowId < maxRowId; rowId++) {
          if (nullBitset.get(rowId)) {
            // means the data for this row is null
            vector.putNull(vectorOffset++);
          } else {
            byte[] data = columnPage.getBytes(rowId);
            vector.putBytes(vectorOffset++, 0, data.length, data);
          }
        }
        break;
      default:
        throw new RuntimeException("unsupported data type: " + columnPage.getDataType());
    }

    return column + 1;
  }

  @Override
  public int fillConvertedChunkData(int[] rowMapping, ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offsetRowId = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int maxRowId = offsetRowId + columnVectorInfo.size;
    switch (columnPage.getDataType()) {
      case INT:
        DirectDictionaryGenerator generator = columnVectorInfo.directDictionaryGenerator;
        assert (generator != null);
        DataType dataType = generator.getReturnType();
        assert (dataType == DataType.INT || dataType == DataType.LONG);
        for (int rowId = offsetRowId; rowId < maxRowId; rowId++) {
          int surrogate = columnPage.getInt(rowMapping[rowId]);
          Object valueFromSurrogate = generator.getValueFromSurrogate(surrogate);
          if (valueFromSurrogate == null) {
            vector.putNull(vectorOffset++);
          } else {
            if (dataType == DataType.INT) {
              vector.putInt(vectorOffset++, (int) valueFromSurrogate);
            } else {
              vector.putLong(vectorOffset++, (long) valueFromSurrogate);
            }
          }
        }
        break;
      case STRING:
        BitSet nullBitset = columnPage.getNullBits();
        for (int rowId = offsetRowId; rowId < maxRowId; rowId++) {
          if (nullBitset.get(rowId)) {
            // means the data for this row is null
            vector.putNull(vectorOffset++);
          } else {
            byte[] data = columnPage.getBytes(rowMapping[rowId]);
            vector.putBytes(vectorOffset++, 0, data.length, data);
          }
        }
        break;
      default:
        throw new RuntimeException("unsupported data type: " + columnPage.getDataType());
    }
    return column + 1;
  }

  @Override
  public byte[] getChunkData(int columnIndex) {
    return columnPage.getBytes(columnIndex);
  }

  @Override
  public int getInvertedIndex(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public boolean isNoDicitionaryColumn() {
    return true;
  }

  @Override
  public int getColumnValueSize() {
    return columnValueSize;
  }

  @Override
  public boolean isExplicitSorted() {
    return false;
  }

  @Override
  public int compareTo(int rowId, byte[] compareValue) {
    byte[] data = columnPage.getBytes(rowId);
    return ByteUtil.UnsafeComparer.INSTANCE
        .compareTo(data, 0, data.length, compareValue, 0, compareValue.length);
  }

  @Override
  public void freeMemory() {
    columnPage.freeMemory();
  }

}
