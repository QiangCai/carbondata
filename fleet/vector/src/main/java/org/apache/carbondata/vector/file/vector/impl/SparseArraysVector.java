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

import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.vector.file.reader.impl.SparseArraysReader;
import org.apache.carbondata.vector.file.reader.impl.SparseReader;
import org.apache.carbondata.vector.file.vector.ArrayVector;
import org.apache.carbondata.vector.file.vector.ArrayVectorFactory;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.vectorized.ColumnarArray;

/**
 * vector of sparse array data type array
 */
public class SparseArraysVector extends SparseVector {

  private ArrayVector vector;

  /**
   * Sets up the data type of this column vector.
   *
   * @param type
   */
  public SparseArraysVector(ArrayType type, CarbonColumn column) {
    super(type);
    vector = ArrayVectorFactory.createArrayVector(
        ((CarbonDimension) column).getListOfChildDimensions().get(0));
  }

  @Override
  protected void fillData(SparseReader reader) throws IOException {
    if (numRows > numNulls) {
      // fill child vector
      vector.fillVector(((SparseArraysReader) reader).getChildReader(), (int) (end - start));
    }
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    // TODO better to reuse this ColumnarArray object to reduce GC.
    return new ColumnarArray(vector, offsetAt(rowId), dataLengthAt(rowId));
  }

  @Override
  public void close() {
    super.close();
    if (vector != null) {
      vector.close();
      vector = null;
    }
  }
}
