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

package org.apache.carbondata.spark.spark.indextable;

import java.io.Serializable;
import java.util.List;

public class IndexTableInfo implements Serializable {

  private static final long serialVersionUID = 1106104914918491724L;

  private String databaseName;
  private String tableName;
  private List<String> indexCols;

  public IndexTableInfo(String databaseName, String tableName, List<String> indexCols) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.indexCols = indexCols;
  }

  /**
   * returns db name
   *
   * @return
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * returns table name
   *
   * @return
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * returns all the index columns
   *
   * @return
   */
  public List<String> getIndexCols() {
    return indexCols;
  }

  /**
   * compares both the objects
   *
   * @param obj
   * @return
   */
  @Override public boolean equals(Object obj) {
    if (obj == null) {
      return false;

    }
    if (!(obj instanceof IndexTableInfo)) {
      return false;
    }
    IndexTableInfo other = (IndexTableInfo) obj;
    if (indexCols == null) {
      if (other.indexCols != null) {
        return false;
      }
    } else if (!indexCols.equals(other.indexCols)) {
      return false;
    }
    return true;
  }

  @Override public int hashCode() {
    int hashCode = 0;
    for (String s : indexCols) {
      hashCode += s.hashCode();
    }
    return hashCode;
  }
}
