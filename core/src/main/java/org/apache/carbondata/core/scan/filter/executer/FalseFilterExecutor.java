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

package org.apache.carbondata.core.scan.filter.executer;

import java.util.BitSet;

import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;

public class FalseFilterExecutor implements FilterExecuter {

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawChunks, boolean useBitsetPipeline) {
    int numberOfPages = rawChunks.getDataBlock().numberOfPages();
    BitSetGroup group = new BitSetGroup(numberOfPages);
    for (int i = 0; i < numberOfPages; i++) {
      BitSet set = new BitSet();
      group.setBitSet(set, i);
    }
    return group;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawChunks) {
    int numberOfPages = rawChunks.getDataBlock().numberOfPages();
    return new BitSet(numberOfPages);
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax) {
    return false;
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    return new BitSet();
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks blockChunkHolder) {
    // Do Nothing
  }
}
