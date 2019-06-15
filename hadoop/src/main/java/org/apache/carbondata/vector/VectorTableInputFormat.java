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

package org.apache.carbondata.vector;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;

/**
 * reader util class of vector table
 */

public class VectorTableInputFormat {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(VectorTableInputFormat.class.getCanonicalName());

  public static List<InputSplit> getSplit(List<Segment> segments) {
    if (segments == null || segments.isEmpty()) {
      return new ArrayList<InputSplit>(0);
    }
    List<InputSplit> splits = new ArrayList<InputSplit>(segments.size());
    for (Segment segment : segments) {
      splits.add(new CarbonInputSplit(
          segment.getSegmentNo(),
          CarbonTablePath.SEGMENT_PREFIX + segment.getSegmentNo(),
          0,
          0,
          new String[0],
          FileFormat.VECTOR_V1));
    }
    return splits;
  }

  public static RecordReader<Void, Object> createRecordReader(QueryModel queryModel,
      Configuration hadoopConf, boolean enableBatch) throws Exception {
    String name = "org.apache.carbondata.vector.table.VectorSplitReader";
    try {
      Constructor<?>[] cons = Class.forName(name).getDeclaredConstructors();
      cons[0].setAccessible(true);
      return (RecordReader<Void, Object>) cons[0].newInstance(
          queryModel, hadoopConf, enableBatch);
    } catch (Exception e) {
      LOGGER.error(e);
      throw e;
    }
  }
}
