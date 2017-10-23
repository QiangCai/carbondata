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

package org.apache.carbondata.segment;

import org.apache.carbondata.file.mapreduce.CarbonRowStoreOutputFormat;

public class RowStoreSegmentOutputFormat extends CarbonRowStoreOutputFormat {

  public enum AppendType {
    APPEND, MOVE
  }

  public enum PlanStatus {
    IN_PROGRESS, FINISHED
  }

  public static class AppendPlan {
    private AppendType appendType;
    // relative path
    private String targetFile;
    // source file name
    private String sourceFile;
    // original target file size
    private long targetFileSize;
    // latest target file size
    private long targetFileSizeNew;
    // source file size
    private long sourceFileSize;
    // plan status
    private PlanStatus planStatus;

    public AppendPlan(String sourceFile, long sourceFileSize) {
      this.appendType = AppendType.MOVE;
      this.sourceFile = sourceFile;
      this.sourceFileSize = sourceFileSize;
      this.targetFile = sourceFile;
      this.targetFileSize = 0;
      this.targetFileSizeNew = 0;
      this.planStatus = PlanStatus.IN_PROGRESS;
    }

    public AppendPlan(String sourceFile, long sourceFileSize, String targetFile,
        long targetFileSize) {
      this.appendType = AppendType.APPEND;
      this.sourceFile = sourceFile;
      this.sourceFileSize = sourceFileSize;
      this.targetFile = targetFile;
      this.targetFileSize = targetFileSize;
      this.targetFileSizeNew = targetFileSize;
    }

    public AppendType getAppendType() {
      return appendType;
    }

    public void setAppendType(AppendType appendType) {
      this.appendType = appendType;
    }

    public String getTargetFile() {
      return targetFile;
    }

    public void setTargetFile(String targetFile) {
      this.targetFile = targetFile;
    }

    public String getSourceFile() {
      return sourceFile;
    }

    public void setSourceFile(String sourceFile) {
      this.sourceFile = sourceFile;
    }

    public long getTargetFileSize() {
      return targetFileSize;
    }

    public void setTargetFileSize(long targetFileSize) {
      this.targetFileSize = targetFileSize;
    }

    public long getSourceFileSize() {
      return sourceFileSize;
    }

    public void setSourceFileSize(long sourceFileSize) {
      this.sourceFileSize = sourceFileSize;
    }

    public PlanStatus getPlanStatus() {
      return planStatus;
    }

    public void setPlanStatus(PlanStatus planStatus) {
      this.planStatus = planStatus;
    }

    public long getTargetFileSizeNew() {
      return targetFileSizeNew;
    }

    public void setTargetFileSizeNew(long targetFileSizeNew) {
      this.targetFileSizeNew = targetFileSizeNew;
    }
  }
}
