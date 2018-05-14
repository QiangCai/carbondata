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

package org.apache.carbondata.ai.algorithm.impl;

import org.apache.carbondata.ai.algorithm.Algorithm;
import org.apache.carbondata.ai.model.Feature;
import org.apache.carbondata.ai.model.FeatureSet;

public class KNNSearch implements Algorithm<int[][], float[]> {

  private Feature<float[]> search;

  public KNNSearch(Feature search) {
    this.search = search;
  }

  @Override public String getShortName() {
    return "KNNSearch";
  }

  @Override public float[] execute(FeatureSet<int[][]> featureSet) {
    float[] feature = search.getValue();
    int dimensionNums = feature.length;
    int[][] featureSetValues = featureSet.getValues();
    int length = featureSet.getLength();
    float[] distances = new float[length];
    for (int i = 0; i < length; i++) {
      double distance = 0;
      for (int j = 0; j < dimensionNums; j++) {
        distance += Math.pow(feature[j] - featureSetValues[i][j], 2);
      }
      distances[i] = (float) Math.sqrt(distance);
    }
    return distances;
  }
}
