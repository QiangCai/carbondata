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

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.vector.file.reader.impl.SparseMapsReader;
import org.apache.carbondata.vector.file.reader.impl.SparseReader;
import org.apache.carbondata.vector.file.vector.ArrayVector;
import org.apache.carbondata.vector.file.vector.ArrayVectorFactory;

import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.vectorized.ColumnarMap;

/**
 * vector of sparse map data type array
 */
public class SparseMapsVector extends SparseVector {

  private ArrayVector keyVector;
  private ArrayVector valueVector;

  /**
   * Sets up the data type of this column vector.
   *
   * @param type
   */
  public SparseMapsVector(MapType type, CarbonColumn column) {
    super(type);
    List<CarbonDimension> childDimensions =
        ((CarbonDimension) column).getListOfChildDimensions().get(0).getListOfChildDimensions();
    keyVector = ArrayVectorFactory.createArrayVector(childDimensions.get(0));
    valueVector = ArrayVectorFactory.createArrayVector(childDimensions.get(0));
  }

  @Override
  protected void fillData(SparseReader reader) throws IOException {
    if (numRows > numNulls) {
      int length = (int) (end - start);
      keyVector.fillVector(((SparseMapsReader) reader).getKeyReader(), length);
      valueVector.fillVector(((SparseMapsReader) reader).getValueReader(), length);
    }
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    // TODO better to reuse this ColumnarMap object to reduce GC.
    return new ColumnarMap(keyVector, valueVector, offsetAt(ordinal), dataLengthAt(ordinal));
  }

  @Override
  public void close() {
    super.close();
    if (keyVector != null) {
      keyVector.close();
      keyVector = null;
    }
    if (valueVector != null) {
      valueVector.close();
      valueVector = null;
    }
  }
}
