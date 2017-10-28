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

package org.apache.carbondata.hadoop.util;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.complextypes.ArrayQueryType;
import org.apache.carbondata.core.scan.complextypes.PrimitiveQueryType;
import org.apache.carbondata.core.scan.complextypes.StructQueryType;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.intf.FilterOptimizer;
import org.apache.carbondata.core.scan.filter.intf.FilterOptimizerBasic;
import org.apache.carbondata.core.scan.filter.optimizer.RangeFilterOptmizer;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.processing.loading.DataField;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * Utility class
 */
public class CarbonInputFormatUtil {

  public static CarbonQueryPlan createQueryPlan(CarbonTable carbonTable, String columnString) {
    String[] columns = null;
    if (columnString != null) {
      columns = columnString.split(",");
    }
    String factTableName = carbonTable.getFactTableName();
    CarbonQueryPlan plan = new CarbonQueryPlan(carbonTable.getDatabaseName(), factTableName);
    // fill dimensions
    // If columns are null, set all dimensions and measures
    int i = 0;
    if (columns != null) {
      for (String column : columns) {
        CarbonDimension dimensionByName = carbonTable.getDimensionByName(factTableName, column);
        if (dimensionByName != null) {
          addQueryDimension(plan, i, dimensionByName);
          i++;
        } else {
          CarbonMeasure measure = carbonTable.getMeasureByName(factTableName, column);
          if (measure == null) {
            throw new RuntimeException(column + " column not found in the table " + factTableName);
          }
          addQueryMeasure(plan, i, measure);
          i++;
        }
      }
    }

    plan.setQueryId(System.nanoTime() + "");
    return plan;
  }

  public static <V> CarbonTableInputFormat<V> createCarbonInputFormat(
      AbsoluteTableIdentifier identifier,
      Job job) throws IOException {
    CarbonTableInputFormat<V> carbonInputFormat = new CarbonTableInputFormat<>();
    FileInputFormat.addInputPath(job, new Path(identifier.getTablePath()));
    return carbonInputFormat;
  }

  public static <V> CarbonTableInputFormat<V> createCarbonTableInputFormat(
      AbsoluteTableIdentifier identifier, List<String> partitionId, Job job) throws IOException {
    CarbonTableInputFormat<V> carbonTableInputFormat = new CarbonTableInputFormat<>();
    carbonTableInputFormat.setPartitionIdList(job.getConfiguration(), partitionId);
    FileInputFormat.addInputPath(job, new Path(identifier.getTablePath()));
    return carbonTableInputFormat;
  }

  private static void addQueryMeasure(CarbonQueryPlan plan, int order, CarbonMeasure measure) {
    QueryMeasure queryMeasure = new QueryMeasure(measure.getColName());
    queryMeasure.setQueryOrder(order);
    queryMeasure.setMeasure(measure);
    plan.addMeasure(queryMeasure);
  }

  private static void addQueryDimension(CarbonQueryPlan plan, int order,
      CarbonDimension dimension) {
    QueryDimension queryDimension = new QueryDimension(dimension.getColName());
    queryDimension.setQueryOrder(order);
    queryDimension.setDimension(dimension);
    plan.addDimension(queryDimension);
  }

  public static void processFilterExpression(Expression filterExpression, CarbonTable carbonTable) {
    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getFactTableName());
    List<CarbonMeasure> measures =
        carbonTable.getMeasureByTableName(carbonTable.getFactTableName());
    QueryModel.processFilterExpression(filterExpression, dimensions, measures);

    if (null != filterExpression) {
      // Optimize Filter Expression and fit RANGE filters is conditions apply.
      FilterOptimizer rangeFilterOptimizer =
          new RangeFilterOptmizer(new FilterOptimizerBasic(), filterExpression);
      rangeFilterOptimizer.optimizeFilter();
    }
  }

  /**
   * Resolve the filter expression.
   *
   * @param filterExpression
   * @param absoluteTableIdentifier
   * @return
   */
  public static FilterResolverIntf resolveFilter(Expression filterExpression,
      AbsoluteTableIdentifier absoluteTableIdentifier, TableProvider tableProvider) {
    try {
      FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
      //get resolved filter
      return filterExpressionProcessor
          .getFilterResolver(filterExpression, absoluteTableIdentifier, tableProvider);
    } catch (Exception e) {
      throw new RuntimeException("Error while resolving filter expression", e);
    }
  }

  public static GenericQueryType[] getComplexDimensions(CarbonTable carbontable,
      CarbonColumn[] carbonColumns, Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache)
      throws IOException {
    GenericQueryType[] queryTypes = new GenericQueryType[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      if (carbonColumns[i].isComplex()) {
        if (carbonColumns[i].getDataType() == DataTypes.ARRAY) {
          queryTypes[i] = new ArrayQueryType(carbonColumns[i].getColName(),
              carbonColumns[i].getColName(), i);
        } else if (carbonColumns[i].getDataType() == DataTypes.STRUCT) {
          queryTypes[i] = new StructQueryType(carbonColumns[i].getColName(),
              carbonColumns[i].getColName(), i);
        } else {
          throw new UnsupportedOperationException(
              carbonColumns[i].getDataType().getName() + " is not supported");
        }

        fillChildren(carbontable, queryTypes[i], (CarbonDimension) carbonColumns[i], i, cache);
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
