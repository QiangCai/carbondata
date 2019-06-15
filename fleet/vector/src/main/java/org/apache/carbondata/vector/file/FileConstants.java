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

package org.apache.carbondata.vector.file;

/**
 * constants in vector file
 */
public class FileConstants {

  /**
   * the data size of load batch, unit: byte
   */
  public static final long TABLE_LOAD_BATCH_SIZE = 256L * 1024 * 1024;

  /**
   * the number of rows in read batch , unit: row
   */
  public static final int FILE_READ_BACTH_ROWS = 100;

  /**
   * the minimum size of reading data file
   */
  public static final int FILE_READ_MIN_SIZE = 4 * 1024;
}
