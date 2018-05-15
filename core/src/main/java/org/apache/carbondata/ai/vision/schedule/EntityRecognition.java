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

package org.apache.carbondata.ai.vision.schedule;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.ai.algorithm.Algorithm;
import org.apache.carbondata.ai.algorithm.AlgorithmFactory;
import org.apache.carbondata.ai.model.EntitySet;
import org.apache.carbondata.ai.model.Entity;
import org.apache.carbondata.ai.model.Model;
import org.apache.carbondata.ai.model.impl.FeatureSetInts;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

public class EntityRecognition {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(EntityRecognition.class.getName());

  private Entity<float[]> entity;
  private Algorithm<int[][], float[]> algorithm;
  private int limit;
  private int[][] batchValues;
  private int rowCount = 0;
  private EntitySet<int[][]> entitySet;
  private Method arrayMethod;

  public EntityRecognition(Model model) {
    this.entity = new Entity<float[]>(model.getParameter());
    this.limit = model.getLimit();
    entitySet = new EntitySet(new FeatureSetInts());
    if ("KNNSearch".equalsIgnoreCase(model.getAlgorithmName())) {
      algorithm = AlgorithmFactory.getKNNSearch(entity.getFeature(), "1.0");
    }

    try {
      Class<?> clazz = Class.forName("org.apache.spark.sql.catalyst.util.GenericArrayData");
      arrayMethod = clazz.getDeclaredMethod("array");
      arrayMethod.setAccessible(true);
    } catch (ClassNotFoundException e) {
      LOGGER.error(e, "Class GenericArrayData not found");
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }

  }

  public void init(int batchSize) {
    if (batchValues == null) {
      batchValues = new int[batchSize][];
      entitySet.getFeatureSet().setValues(batchValues);
      LOGGER.audit("EntityRecognition batchSize: " + batchSize);
    }
    rowCount = 0;
  }

  public void add(Object value) {
    try {
      Object[] objects = (Object[]) arrayMethod.invoke(value);
      int[] unboxedValues = new int[objects.length];
      for (int i = 0; i < objects.length; i++) {
        unboxedValues[i] = (Integer) objects[i];
      }
      batchValues[rowCount] = unboxedValues;
      rowCount++;
    } catch (InvocationTargetException e) {

    } catch (IllegalAccessException e) {

    }
  }

  public List<Object[]> recognition(List<Object[]> rows, int columnCount) {
    long startTime = System.currentTimeMillis();
    entitySet.setLength(rowCount);
    float[] result = algorithm.execute(entitySet.getFeatureSet());
    List<Object[]> newRows = getTopN(result, rows, columnCount);
    long endTime = System.currentTimeMillis();
    LOGGER.audit("EntityRecognition recognition taken time: " + (endTime - startTime) + " ms");
    return newRows;
  }

  private List<Object[]> getTopN(float[] result, List<Object[]> rows, int columnCount) {
    if (result.length > limit) {
      float smallestNumber = findSmallestNumber(result, result.length, limit);
      List<Object[]> newRows = new ArrayList<Object[]>(limit);
      for (int i = 0; i < result.length; i++) {
        if (result[i] <= smallestNumber) {
          Object[] row = rows.get(i);
          row[columnCount] = result[i];
          newRows.add(row);
        }
      }
      return newRows;
    } else {
      for (int i = 0; i < result.length; i++) {
        rows.get(i)[columnCount] = result[i];
      }
      return rows;
    }
  }

  private float findSmallestNumber(float[] values, int length, int limit) {
    float tmp = values[length / 2];
    int count = 0;
    float[] lessValues = new float[length];
    int lessValuesPoint = 0;
    float[] greaterValues = new float[length];
    int greaterValuesPoint = 0;
    for (int i = 0; i < length; i++) {
      if (values[i] < tmp) {
        lessValues[lessValuesPoint] = values[i];
        lessValuesPoint++;
      } else if (values[i] > tmp) {
        greaterValues[greaterValuesPoint] = values[i];
        greaterValuesPoint++;
      } else {
        count++;
      }
    }
    if (lessValuesPoint > limit) {
      return findSmallestNumber(lessValues, lessValuesPoint, limit);
    } else if (lessValuesPoint + count >= limit) {
      return tmp;
    } else if (lessValuesPoint + count < limit) {
      return findSmallestNumber(greaterValues, greaterValuesPoint, limit - lessValuesPoint - count);
    }
    return tmp;
  }

}
