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

package org.apache.carbondata.core.scan.collector.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.ai.model.EntitySet;
import org.apache.carbondata.ai.model.impl.FeatureSetInts;
import org.apache.carbondata.ai.vision.schedule.EntityRecognition;
import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;

/**
 * This class append blocklet id, page id and row id to each row.
 */
@InterfaceAudience.Internal
public class PushDownUdfResultCollector extends DictionaryBasedResultCollector {

  private EntityRecognition entityRecognition;
  private boolean initialized = false;

  public PushDownUdfResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
    if (blockExecutionInfos.usingAiModel()) {
      entityRecognition = new EntityRecognition(blockExecutionInfos.getAiModel());
    }
  }

  @Override
  public List<Object[]> collectResultInRow(BlockletScannedResult scannedResult, int batchSize) {

    if (!initialized) {
      initialized = true;
      entityRecognition.init(batchSize);
    }
    // scan the record and add to list
    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
    int rowCounter = 0;
    int[] surrogateResult;
    byte[][] noDictionaryKeys;
    byte[][] complexTypeKeyArray;
    int columnCount = queryDimensions.length + queryMeasures.length;
    while (scannedResult.hasNext() && rowCounter < batchSize) {
      Object[] row = new Object[columnCount + 1];
      if (isDimensionExists) {
        surrogateResult = scannedResult.getDictionaryKeyIntegerArray();
        noDictionaryKeys = scannedResult.getNoDictionaryKeyArray();
        complexTypeKeyArray = scannedResult.getComplexTypeKeyArray();
        dictionaryColumnIndex = 0;
        noDictionaryColumnIndex = 0;
        complexTypeColumnIndex = 0;
        for (int i = 0; i < queryDimensions.length; i++) {
          fillDimensionData(scannedResult, surrogateResult, noDictionaryKeys, complexTypeKeyArray,
              comlexDimensionInfoMap, row, i);
        }
      } else {
        scannedResult.incrementCounter();
      }
      if (scannedResult.containsDeletedRow(scannedResult.getCurrentRowId())) {
        continue;
      }
      fillMeasureData(scannedResult, row);
      listBasedResult.add(row);
      rowCounter++;
      if (entityRecognition != null) {
        entityRecognition.add(row[columnCount - 1]);
      }
    }
    if (entityRecognition != null) {
      entityRecognition.recognition(listBasedResult, columnCount);
    }
    return listBasedResult;
  }

}
