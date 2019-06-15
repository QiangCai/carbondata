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
import org.apache.carbondata.vector.file.reader.ArrayReader;
import org.apache.carbondata.vector.file.reader.impl.SparseReader;
import org.apache.carbondata.vector.file.reader.impl.SparseStructsReader;
import org.apache.carbondata.vector.file.vector.ArrayVector;
import org.apache.carbondata.vector.file.vector.ArrayVectorFactory;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * vector of sparse struct data type array
 */
public class SparseStructsVector extends SparseVector {

  private ArrayVector[] childVectors;
  private final int numColumns;

  /**
   * Sets up the data type of this column vector.
   *
   * @param type
   */
  public SparseStructsVector(StructType type, CarbonColumn column) {
    super(type);
    List<CarbonDimension> childDimensions = ((CarbonDimension) column).getListOfChildDimensions();
    numColumns = childDimensions.size();
    childVectors =new ArrayVector[numColumns];
    for (int index = 0; index < numColumns; index++) {
      childVectors[index] = ArrayVectorFactory.createArrayVector(childDimensions.get(index));
    }
  }

  @Override
  protected void fillData(SparseReader reader) throws IOException {
    if (numRows > numNulls) {
      int length = (int) (end - start);
      ArrayReader[] childReaders = ((SparseStructsReader) reader).getChildReaders();
      for (int index = 0; index < numColumns; index++) {
        childVectors[index].fillVector(childReaders[index], length);
      }
    }
  }

  @Override
  protected ColumnVector getChild(int ordinal) {
    return childVectors[ordinal];
  }

  @Override
  public void close() {
    super.close();
    if (childVectors != null) {
      for (int index = 0; index < numColumns; index++) {
        if (childVectors[index] != null) {
          childVectors[index].close();
          childVectors[index] = null;
        }
      }
      childVectors = null;
    }
  }
}
