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
  private int[][] batchValues;
  private int rowCount = 0;
  private EntitySet<int[][]> entitySet;
  private Method arrayMethod;


  public EntityRecognition(Model model) {
    this.entity = new Entity<float[]>(model.getParameter());
    entitySet = new EntitySet(new FeatureSetInts());
    if ("KNNSearch".equalsIgnoreCase(model.getAlgorithmName())) {
      algorithm = AlgorithmFactory.getKNNSearch(entity.getFeature(), "1.0");
    }

    try {
      Class<?> clazz = Class.forName("org.apache.spark.sql.catalyst.util.GenericArrayData");
      arrayMethod = clazz.getDeclaredMethod ("array");
      arrayMethod.setAccessible(true);
    } catch (ClassNotFoundException e) {
      LOGGER.error(e, "Class GenericArrayData not found");
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }

  }

  public void init(int batchSize) {
    batchValues = new int[batchSize][];
    entitySet.getFeatureSet().setValues(batchValues);
  }

  public void add(Object value) {
    try {
      Object[] objects = (Object[])arrayMethod.invoke(value);
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

  public void recognition(List<Object[]> rows, int columnCount) {
    entitySet.setLength(rowCount);
    float[] result = algorithm.execute(entitySet.getFeatureSet());
    for(int i = 0; i < result.length; i++) {
      rows.get(i)[columnCount] = result[i];
    }
  }
}
