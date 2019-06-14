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

package org.apache.carbondata.vector.file.vector;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * adapter of ColumnVector
 */
@InterfaceAudience.Internal
@InterfaceStability.Evolving
public abstract class ArrayVector extends ColumnVector {

  /**
   * Sets up the data type of this column vector.
   *
   * @param type
   */
  public ArrayVector(DataType type) {
    super(type);
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public byte getByte(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public short getShort(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public int getInt(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public long getLong(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public float getFloat(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public double getDouble(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new RuntimeException("unsupported operation");
  }

  @Override
  protected ColumnVector getChild(int ordinal) {
    throw new RuntimeException("unsupported operation");
  }
}
