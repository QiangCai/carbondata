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

package org.apache.carbondata.core.datastore.chunk.impl;

import java.util.function.IntUnaryOperator;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory.DimensionStoreType;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * This class is gives access to fixed length dimension data chunk store
 */
public class FixedLengthDimensionColumnPage extends AbstractDimensionColumnPage {

  /**
   * Constructor
   *
   * @param dataChunk            data chunk
   * @param invertedIndex        inverted index
   * @param invertedIndexReverse reverse inverted index
   * @param numberOfRows         number of rows
   * @param columnValueSize      size of each column value
   */
  public FixedLengthDimensionColumnPage(byte[] dataChunk, int[] invertedIndex,
      int[] invertedIndexReverse, int numberOfRows, int columnValueSize, int dataLength) {
    boolean isExplicitSorted = isExplicitSorted(invertedIndex);
    long totalSize = isExplicitSorted ?
        dataLength + (2 * numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE) :
        dataLength;
    dataChunkStore = DimensionChunkStoreFactory.INSTANCE
        .getDimensionChunkStore(columnValueSize, isExplicitSorted, numberOfRows, totalSize,
            DimensionStoreType.FIXED_LENGTH, null, false, dataLength);
    dataChunkStore.putArray(invertedIndex, invertedIndexReverse, dataChunk);
  }

  /**
   * Constructor
   *
   * @param dataChunk            data chunk
   * @param invertedIndex        inverted index
   * @param invertedIndexReverse reverse inverted index
   * @param numberOfRows         number of rows
   * @param columnValueSize      size of each column value
   * @param vectorInfo           vector to be filled with decoded column page.
   */
  public FixedLengthDimensionColumnPage(byte[] dataChunk, int[] invertedIndex,
      int[] invertedIndexReverse, int numberOfRows, int columnValueSize,
      ColumnVectorInfo vectorInfo, int dataLength) {
    boolean isExplicitSorted = isExplicitSorted(invertedIndex);
    long totalSize = isExplicitSorted ?
        dataLength + (2 * numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE) :
        dataLength;
    dataChunkStore = DimensionChunkStoreFactory.INSTANCE
        .getDimensionChunkStore(columnValueSize, isExplicitSorted, numberOfRows, totalSize,
            DimensionStoreType.FIXED_LENGTH, null, vectorInfo != null, dataLength);
    if (vectorInfo == null) {
      dataChunkStore.putArray(invertedIndex, invertedIndexReverse, dataChunk);
    } else {
      dataChunkStore.fillVector(invertedIndex, invertedIndexReverse, dataChunk, vectorInfo);
    }
  }

  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param rowId            row id of the chunk
   * @param offset           offset from which data need to be filed
   * @param data             data to filed
   * @return how many bytes was copied
   */
  @Override
  public int fillRawData(int rowId, int offset, byte[] data) {
    dataChunkStore.fillRow(rowId, data, offset);
    return dataChunkStore.getColumnValueSize();
  }

  /**
   * Converts to column dictionary integer value
   *
   * @param rowId
   * @param chunkIndex
   * @param outputSurrogateKey
   * @return
   */
  @Override
  public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
    outputSurrogateKey[chunkIndex] = dataChunkStore.getSurrogate(rowId);
    return chunkIndex + 1;
  }

  private int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex,
      IntUnaryOperator function) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    for (int j = offset; j < len; j++) {
      int dict = dataChunkStore.getSurrogate(function.applyAsInt(j));
      if (columnVectorInfo.directDictionaryGenerator == null) {
        vector.putInt(vectorOffset++, dict);
      } else {
        Object valueFromSurrogate =
            columnVectorInfo.directDictionaryGenerator.getValueFromSurrogate(dict);
        if (valueFromSurrogate == null) {
          vector.putNull(vectorOffset++);
        } else {
          DataType dataType = columnVectorInfo.directDictionaryGenerator.getReturnType();
          if (dataType == DataTypes.INT) {
            vector.putInt(vectorOffset++, (int) valueFromSurrogate);
          } else if (dataType == DataTypes.LONG) {
            vector.putLong(vectorOffset++, (long) valueFromSurrogate);
          } else {
            throw new IllegalArgumentException(
                "unsupported data type: " + columnVectorInfo.directDictionaryGenerator
                    .getReturnType());
          }
        }
      }
    }
    return chunkIndex + 1;
  }


  /**
   * Fill the data to vector
   *
   * @param vectorInfo
   * @param chunkIndex
   * @return next column index
   */
  @Override
  public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    return fillVector(vectorInfo, chunkIndex, j -> j);
  }

  /**
   * Fill the data to vector
   *
   * @param filteredRowId
   * @param vectorInfo
   * @param chunkIndex
   * @return next column index
   */
  @Override
  public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo,
      int chunkIndex) {
    return fillVector(vectorInfo, chunkIndex, j -> filteredRowId[j]);
  }
}
