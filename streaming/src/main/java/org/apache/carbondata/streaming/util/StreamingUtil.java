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

package org.apache.carbondata.streaming.util;

import java.io.IOException;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.complextypes.ArrayQueryType;
import org.apache.carbondata.core.scan.complextypes.PrimitiveQueryType;
import org.apache.carbondata.core.scan.complextypes.StructQueryType;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.processing.loading.DataField;

public class StreamingUtil {

  public static GenericQueryType[] getComplexDimensions(CarbonTable carbontable,
      DataField[] dataFields, Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache)
      throws IOException {
    GenericQueryType[] queryTypes = new GenericQueryType[dataFields.length];
    for (int i = 0; i < dataFields.length; i++) {
      if (dataFields[i].getColumn().isComplex()) {
        if (dataFields[i].getColumn().getDataType() == DataTypes.ARRAY) {
          queryTypes[i] = new ArrayQueryType(dataFields[i].getColumn().getColName(),
              dataFields[i].getColumn().getColName(), i);
        } else if (dataFields[i].getColumn().getDataType() == DataTypes.STRUCT) {
          queryTypes[i] = new StructQueryType(dataFields[i].getColumn().getColName(),
              dataFields[i].getColumn().getColName(), i);
        } else {
          throw new UnsupportedOperationException(
              dataFields[i].getColumn().getDataType().getName() + " is not supported");
        }

        fillChildren(carbontable, queryTypes[i], (CarbonDimension) dataFields[i].getColumn(), i,
            cache);
      }
    }

    return queryTypes;
  }

  private static void fillChildren(CarbonTable carbontable, GenericQueryType parentQueryType,
      CarbonDimension dimension, int parentBlockIndex,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache) throws IOException {
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      CarbonDimension child = dimension.getListOfChildDimensions().get(i);
      DataType dataType = child.getDataType();
      GenericQueryType queryType = null;
      if (dataType == DataTypes.ARRAY) {
        queryType =
            new ArrayQueryType(child.getColName(), dimension.getColName(), ++parentBlockIndex);

      } else if (dataType == DataTypes.STRUCT) {
        queryType =
            new StructQueryType(child.getColName(), dimension.getColName(), ++parentBlockIndex);
        parentQueryType.addChildren(queryType);
      } else {
        boolean isDirectDictionary =
            CarbonUtil.hasEncoding(child.getEncoder(), Encoding.DIRECT_DICTIONARY);
        DictionaryColumnUniqueIdentifier dictionarIdentifier =
            new DictionaryColumnUniqueIdentifier(carbontable.getCarbonTableIdentifier(),
                child.getColumnIdentifier(), child.getDataType(),
                CarbonStorePath.getCarbonTablePath(carbontable.getAbsoluteTableIdentifier()));

        queryType =
            new PrimitiveQueryType(child.getColName(), dimension.getColName(), ++parentBlockIndex,
                child.getDataType(), Integer.MAX_VALUE, cache.get(dictionarIdentifier),
                isDirectDictionary);
      }
      parentQueryType.addChildren(queryType);
      if (child.getNumberOfChild() > 0) {
        fillChildren(carbontable, queryType, child, parentBlockIndex, cache);
      }
    }
  }
}
