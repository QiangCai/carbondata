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

package org.apache.carbondata.core.datastore.page;

import java.math.BigDecimal;

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;

import static org.apache.carbondata.core.metadata.datatype.DataType.STRING;

// This extension uses unsafe memory to store page data, for variable length data type (string,
// decimal)
public class UnsafeVarLengthColumnPage extends VarLengthColumnPageBase {

  // memory allocated by Unsafe
  private MemoryBlock memoryBlock;

  // base address of memoryBlock
  private Object baseAddress;

  // base offset of memoryBlock
  private long baseOffset;

  // size of the allocated memory, in bytes
  private int capacity;

  // default size for each row, grows as needed
  private static final int DEFAULT_ROW_SIZE = 8;

  private static final double FACTOR = 1.25;

  private final long taskId = ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId();

  /**
   * create a page
   * @param dataType data type
   * @param pageSize number of row
   */
  UnsafeVarLengthColumnPage(DataType dataType, int pageSize, int scale, int precision)
      throws MemoryException {
    super(dataType, pageSize, scale, precision);
    capacity = (int) (pageSize * DEFAULT_ROW_SIZE * FACTOR);
    memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, (long) (capacity));
    baseAddress = memoryBlock.getBaseObject();
    baseOffset = memoryBlock.getBaseOffset();
  }

  /**
   * create a page with initial capacity
   * @param dataType data type
   * @param pageSize number of row
   * @param capacity initial capacity of the page, in bytes
   */
  UnsafeVarLengthColumnPage(DataType dataType, int pageSize, int capacity,
      int scale, int precision) throws MemoryException {
    super(dataType, pageSize, scale, precision);
    this.capacity = capacity;
    memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, (long)(capacity));
    baseAddress = memoryBlock.getBaseObject();
    baseOffset = memoryBlock.getBaseOffset();
  }

  @Override
  public void freeMemory() {
    if (memoryBlock != null) {
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = null;
      baseAddress = null;
      baseOffset = 0;
    }
  }

  /**
   * reallocate memory if capacity length than current size + request size
   */
  private void ensureMemory(int requestSize) throws MemoryException {
    if (totalLength + requestSize > capacity) {
      int newSize = 2 * capacity;
      MemoryBlock newBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, newSize);
      CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset,
          newBlock.getBaseObject(), newBlock.getBaseOffset(), capacity);
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = newBlock;
      baseAddress = newBlock.getBaseObject();
      baseOffset = newBlock.getBaseOffset();
      capacity = newSize;
    }
  }

  @Override
  public void putBytesAtRow(int rowId, byte[] bytes) {
    putBytes(rowId, bytes, 0, bytes.length);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    try {
      ensureMemory(length);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    CarbonUnsafe.getUnsafe().copyMemory(bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET + offset,
        baseAddress, baseOffset + rowOffset[rowId], length);
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    if (totalLength != 0) {
      throw new IllegalStateException("page is not empty");
    }
    for (int i = 0; i < byteArray.length; i++) {
      putBytes(i, byteArray[i]);
    }
  }

  @Override public void putDecimal(int rowId, BigDecimal decimal) {
    putBytes(rowId, decimalConverter.convert(decimal));
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    int length = rowOffset[rowId + 1] - rowOffset[rowId];
    byte[] bytes = new byte[length];
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset + rowOffset[rowId],
        bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);

    return decimalConverter.getDecimal(bytes);
  }

  @Override
  public byte[] getBytes(int rowId) {
    int length = rowOffset[rowId + 1] - rowOffset[rowId];
    byte[] bytes = new byte[length];
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset + rowOffset[rowId],
        bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return bytes;
  }

  @Override
  public byte[][] getByteArrayPage() {
    byte[][] bytes = new byte[pageSize][];
    for (int rowId = 0; rowId < pageSize; rowId++) {
      int length = rowOffset[rowId + 1] - rowOffset[rowId];
      byte[] rowData = new byte[length];
      CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset + rowOffset[rowId],
          rowData, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
      bytes[rowId] = rowData;
    }
    return bytes;
  }

  @Override
  public byte[] getDirectFlattenedBytePage() {
    byte[] bytes = new byte[totalLength];
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset, bytes,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, totalLength);
    return bytes;
  }

  @Override
  void copyBytes(int rowId, byte[] dest, int destOffset, int length) {
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset + rowOffset[rowId],
        dest, CarbonUnsafe.BYTE_ARRAY_OFFSET + destOffset, length);
  }

  /**
   * Return a new column page that construct from input byte array and length of each row
   */
  public static ColumnPage newStringPage(byte[] bytes, byte[] lengths) throws MemoryException {
    int pageSize = lengths.length;
    UnsafeVarLengthColumnPage page = new UnsafeVarLengthColumnPage(STRING, pageSize,
        bytes.length, -1, -1);
    CarbonUnsafe.getUnsafe().copyMemory(bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET,
        page.baseAddress, page.baseOffset, bytes.length);
    int size = 0;
    page.rowOffset[0] = 0;
    for (int rowId = 0; rowId < pageSize; rowId++) {
      page.rowOffset[rowId + 1] = page.rowOffset[rowId] + bytes.length;
      size += bytes.length;
    }
    page.totalLength = size;
    return page;
  }

}
