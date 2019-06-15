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
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.vector.file.vector.impl.SparseArraysVector;
import org.apache.carbondata.vector.file.vector.impl.SparseMapsVector;
import org.apache.carbondata.vector.file.vector.impl.SparsePrimitiveVector;
import org.apache.carbondata.vector.file.vector.impl.SparseStructsVector;
import org.apache.carbondata.vector.file.vector.impl.SparseTimestampsVector;

import org.apache.log4j.Logger;
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil;
import org.apache.spark.sql.types.*;

/**
 * factory to use array vector
 */
public class ArrayVectorFactory {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ArrayVectorFactory.class.getCanonicalName());

  /**
   * create array vector for carbon data type
   * @param column
   * @return
   */
  public static ArrayVector createArrayVector(CarbonColumn column) {
    DataType sparkDataType =
        CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(column.getDataType());
    return createArrayVector(sparkDataType, column);
  }

  /**
   * create array vector for spark data type
   * @param dataType
   * @param column
   * @return
   */
  public static ArrayVector createArrayVector(DataType dataType, CarbonColumn column) {
    if (dataType instanceof StringType ||
        dataType instanceof DateType ||
        dataType instanceof BooleanType ||
        dataType instanceof ShortType ||
        dataType instanceof IntegerType ||
        dataType instanceof LongType ||
        dataType instanceof FloatType ||
        dataType instanceof DoubleType ||
        dataType instanceof DecimalType ||
        dataType instanceof BinaryType ||
        dataType instanceof VarcharType) {
      return new SparsePrimitiveVector(dataType);
    } else if (dataType instanceof TimestampType) {
      return new SparseTimestampsVector(dataType);
    } else if (dataType instanceof ArrayType) {
      return new SparseArraysVector((ArrayType) dataType, column);
    } else if (dataType instanceof StructType) {
      return new SparseStructsVector((StructType) dataType, column);
    } else if (dataType instanceof MapType) {
      return new SparseMapsVector((MapType) dataType, column);
    } else {
      String message = "vector table not support data type: " + dataType.typeName();
      LOGGER.error(message);
      throw new RuntimeException(message);
    }
  }
}
