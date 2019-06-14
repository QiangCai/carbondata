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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.vector.file.vector.impl.SparsePrimitiveVector;
import org.apache.carbondata.vector.file.vector.impl.SparseTimestampVector;

import org.apache.log4j.Logger;

import static org.apache.spark.sql.types.DataTypes.*;

public class ArrayVectorFactory {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ArrayVectorFactory.class.getCanonicalName());

  public static ArrayVector getArrayVector(DataType dataType) {
    int id = dataType.getId();
    if (id == DataTypes.STRING.getId()) {
      return new SparsePrimitiveVector(StringType);
    } else if (id == DataTypes.DATE.getId()) {
      return new SparsePrimitiveVector(DateType);
    } else if (id == DataTypes.TIMESTAMP.getId()) {
      return new SparseTimestampVector(TimestampType);
    } else if (id == DataTypes.BOOLEAN.getId()) {
      return new SparsePrimitiveVector(BooleanType);
    } else if (id == DataTypes.SHORT.getId()) {
      return new SparsePrimitiveVector(ShortType);
    } else if (id == DataTypes.INT.getId()) {
      return new SparsePrimitiveVector(IntegerType);
    } else if (id == DataTypes.LONG.getId()) {
      return new SparsePrimitiveVector(LongType);
    } else if (id == DataTypes.FLOAT.getId()) {
      return new SparsePrimitiveVector(FloatType);
    } else if (id == DataTypes.DOUBLE.getId()) {
      return new SparsePrimitiveVector(DoubleType);
    } else if (id == DataTypes.DECIMAL_TYPE_ID) {
      return new SparsePrimitiveVector(createDecimalType());
    } else if (id == DataTypes.BINARY.getId()) {
      return new SparsePrimitiveVector(BinaryType);
    } else if (id == DataTypes.ARRAY_TYPE_ID) {
      return null;
    } else if (id == DataTypes.STRUCT_TYPE_ID) {
      return null;
    } else if (id == DataTypes.MAP_TYPE_ID) {
      return null;
    } else if (id == DataTypes.VARCHAR.getId()) {
      return new SparsePrimitiveVector(StringType);
    } else {
      throw new RuntimeException(
          "vector table not support data type: " + dataType.getName());
    }
  }

}
