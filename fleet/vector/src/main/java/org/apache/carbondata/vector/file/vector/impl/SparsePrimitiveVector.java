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

import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * sparse primitive data type vector
 */
public class SparsePrimitiveVector extends SparseVector {
  /**
   * Sets up the data type of this column vector.
   *
   * @param type
   */
  public SparsePrimitiveVector(DataType type) {
    super(type);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return data[dataOffset(rowId, 1)] == 1;
  }

  @Override
  public byte getByte(int rowId) {
    return offset[dataOffset(rowId, 1)];
  }

  @Override
  public short getShort(int rowId) {
    int pos = dataOffset(rowId, 2);
    return (short)(((data[pos] & 0xff) << 8) +
        (data[pos + 1] & 0xff));
  }

  @Override
  public int getInt(int rowId) {
    int pos = dataOffset(rowId, 4);
    return ((data[pos] << 24) +
        ((data[pos + 1] & 0xff) << 16) +
        ((data[pos + 2] & 0xff) << 8) +
        (data[pos + 3] & 0xff));
  }

  @Override
  public long getLong(int rowId) {
    int pos = dataOffset(rowId, 8);
    return (((long) data[pos] << 56) +
        ((long) (data[pos + 1] & 255) << 48) +
        ((long) (data[pos + 2] & 255) << 40) +
        ((long) (data[pos + 3] & 255) << 32) +
        ((long) (data[pos + 4] & 255) << 24) +
        ((data[pos + 5] & 255) << 16) +
        ((data[pos + 6] & 255) << 8) +
        ((data[pos + 7] & 255) << 0));
  }

  @Override
  public float getFloat(int rowId) {
    return Float.intBitsToFloat(getInt(rowId));
  }

  @Override
  public double getDouble(int rowId) {
    return Double.longBitsToDouble(getLong(rowId));
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return Decimal.apply(DataTypeUtil.byteToBigDecimal(data, offsetAt(rowId), dataLengthAt(rowId)));
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return UTF8String.fromBytes(data, offsetAt(rowId), dataLengthAt(rowId));
  }

  @Override
  public byte[] getBinary(int rowId) {
    byte[] bytes = new byte[dataLengthAt(rowId)];
    System.arraycopy(data, offsetAt(rowId), bytes, 0, bytes.length);
    return bytes;
  }


}
