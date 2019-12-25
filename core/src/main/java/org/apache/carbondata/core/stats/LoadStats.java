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

package org.apache.carbondata.core.stats;

import java.io.Serializable;

public class LoadStats implements Serializable {

  private int numFiles = 0;

  private long numOutputBytes = 0L;

  private long numOutputRows = 0L;

  public LoadStats() {
  }

  public int getNumFiles() {
    return numFiles;
  }

  public void setNumFiles(int numFiles) {
    this.numFiles = numFiles;
  }

  public long getNumOutputBytes() {
    return numOutputBytes;
  }

  public void setNumOutputBytes(long numOutputBytes) {
    this.numOutputBytes = numOutputBytes;
  }

  public long getNumOutputRows() {
    return numOutputRows;
  }

  public void setNumOutputRows(long numOutputRows) {
    this.numOutputRows = numOutputRows;
  }

  public synchronized void addFiles(int numFiles) {
    this.numFiles += numFiles;
  }

  public synchronized void addOutputBytes(long numOutputBytes) {
    this.numOutputBytes += numOutputBytes;
  }

  public synchronized void addOutputRows(long numOutputRows) {
    this.numOutputRows += numOutputRows;
  }
}
