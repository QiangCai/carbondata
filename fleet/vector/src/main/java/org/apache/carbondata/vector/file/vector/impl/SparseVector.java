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

package org.apache.carbondata.vector.file.vector.impl;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.vector.file.FileConstants;
import org.apache.carbondata.vector.file.vector.ArrayVector;

import org.apache.spark.sql.types.DataType;

/**
 * vector of sparse array data
 */
@InterfaceAudience.Internal
@InterfaceStability.Evolving
public abstract class SparseVector extends ArrayVector {

  protected int numRows = 0;
  protected int numNulls = 0;
  /**
   * the length of bytes in a offset value
   */
  protected int stepLen = 8;

  // offset
  protected byte[] offset;
  /**
   * the valid length of offset
   */
  protected int offsetLen;
  protected long start = 0;
  protected long end = 0;

  // data
  protected byte[] data;

  /**
   * Sets up the data type of this column vector.
   *
   * @param type
   */
  public SparseVector(DataType type) {
    super(type);
  }

  public int fillVector(DataInputStream offsetInput, DataInputStream dataInput,
      int rowCount) throws IOException {
    // reset
    start = end;
    numRows = 0;
    numNulls = 0;
    offsetLen = 0;

    // fill offset and data
    fillOffset(offsetInput, rowCount);
    fillData(dataInput);
    return numRows;
  }

  protected void fillOffset(DataInputStream offsetInput, int rowCount) throws IOException {
    int capacity = rowCount * stepLen;
    allocateOffset(capacity);
    offsetLen = read(offsetInput, offset, capacity);
    if (offsetLen == -1) {
      numRows = -1;
    }
    if (offsetLen >= stepLen) {
      for (int index = 0; index < offsetLen; index += stepLen) {
        numRows += 1;
        if (offset[index] == stepLen) {
          numNulls += 1;
        }
      }
      end = bytesToLong(offset, offsetLen - stepLen);
    }
  }

  protected void fillData(DataInputStream dataInput) throws IOException {
    if (numRows > numNulls) {
      long length = end - start;
      if (length > 0) {
        if (length > Integer.MAX_VALUE) {
          throw new IOException("not support large data, length is " + length);
        }
        allocateData((int) length);
        read(dataInput, data, (int) length);
      }
    }
  }

  private void allocateOffset(int capacity) {
    if (offset == null || offset.length < capacity) {
      offset = new byte[capacity];
    }
  }

  private void allocateData(int capacity) {
    if (data == null || data.length < capacity) {
      data = new byte[dataCapacity(capacity)];
    }
  }

  protected int dataCapacity(int capacity) {
    return Math.max(capacity, FileConstants.FILE_READ_MIN_SIZE);
  }

  protected int dataOffset(int rowId, int byteSize) {
    return (int) (bytesToLong(offset, rowId << 3) - start - byteSize);
  }

  protected int offsetAt(int rowId) {
    if (rowId == 0) {
      return 0;
    } else {
      return (int) (bytesToLong(offset, (rowId - 1) << 3) - start);
    }
  }

  protected int dataLength(int rowId) {
    if (rowId == 0) {
      return (int) (bytesToLong(offset, 0) - start);
    } else {
      return (int) (bytesToLong(offset, rowId << 3) -
          bytesToLong(offset, (rowId - 1) << 3));
    }
  }

  @Override
  public boolean hasNull() {
    return numNulls > 0;
  }

  @Override
  public int numNulls() {
    return numNulls;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return offset[rowId << 3] == 8;
  }

  @Override
  public void close() {
    offset = null;
    data = null;
  }

  // first byte is flag byte
  protected static long bytesToLong(final byte[] bytes, final int start) {
    return ((long)(bytes[start + 1] & 255) << 48) +
        ((long)(bytes[start + 2] & 255) << 40) +
        ((long)(bytes[start + 3] & 255) << 32) +
        ((long)(bytes[start + 4] & 255) << 24) +
        ((bytes[start + 5] & 255) << 16) +
        ((bytes[start + 6] & 255) <<  8) +
        ((bytes[start + 7] & 255) <<  0);
  }

  protected static int read(final DataInputStream input, final byte[] bytes, final int length)
      throws IOException {
    int readLength = input.read(bytes, 0, length);
    if (readLength == -1) {
      return -1;
    }
    int remaining;
    while ((remaining = length - readLength) > 0) {
      int len = input.read(bytes, readLength, remaining);
      if (len == -1) {
        break;
      }
      readLength += len;
    }
    return readLength;
  }



}
