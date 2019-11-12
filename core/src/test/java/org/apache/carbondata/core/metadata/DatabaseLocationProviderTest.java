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
package org.apache.carbondata.core.metadata;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

import org.junit.Assert;
import org.junit.Test;

public class DatabaseLocationProviderTest {

  @Test
  public void testCustomProvider() {
    System.setProperty(
        CarbonCommonConstants.DATABASE_LOCATION_PROVIDER,
        "org.apache.carbondata.core.metadata.DatabaseLocationProviderTest$TestProvider"
    );
    Assert.assertEquals("test", DatabaseLocationProvider.get().provide("databaseName"));
  }

  public static final class TestProvider extends DatabaseLocationProvider {

    @Override
    public String provide(String originalDatabaseName) {
      return "test";
    }

  }

}
