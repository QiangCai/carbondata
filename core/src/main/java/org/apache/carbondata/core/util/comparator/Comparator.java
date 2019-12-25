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

package org.apache.carbondata.core.util.comparator;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

public final class Comparator {

  public static SerializableComparator getComparator(DataType dataType) {
    if (dataType == DataTypes.BOOLEAN) {
      return new BooleanSerializableComparator();
    } else if (dataType == DataTypes.INT) {
      return new IntSerializableComparator();
    } else if (dataType == DataTypes.SHORT) {
      return new ShortSerializableComparator();
    } else if (dataType == DataTypes.DOUBLE) {
      return new DoubleSerializableComparator();
    } else if (dataType == DataTypes.FLOAT) {
      return new FloatSerializableComparator();
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.DATE
        || dataType == DataTypes.TIMESTAMP) {
      return new LongSerializableComparator();
    } else if (DataTypes.isDecimal(dataType)) {
      return new BigDecimalSerializableComparator();
    } else {
      return new ByteArraySerializableComparator();
    }
  }

  /**
   * create Comparator for Measure Datatype
   *
   * @param dataType
   * @return
   */
  public static SerializableComparator getComparatorByDataTypeForMeasure(DataType dataType) {
    if (dataType == DataTypes.BOOLEAN) {
      return new BooleanSerializableComparator();
    } else if (dataType == DataTypes.INT) {
      return new IntSerializableComparator();
    } else if (dataType == DataTypes.SHORT) {
      return new ShortSerializableComparator();
    } else if (dataType == DataTypes.LONG) {
      return new LongSerializableComparator();
    } else if (dataType == DataTypes.DOUBLE) {
      return new DoubleSerializableComparator();
    } else if (dataType == DataTypes.FLOAT) {
      return new FloatSerializableComparator();
    } else if (DataTypes.isDecimal(dataType)) {
      return new BigDecimalSerializableComparator();
    } else if (dataType == DataTypes.BYTE) {
      return new ByteArraySerializableComparator();
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + dataType.getName());
    }
  }
}

