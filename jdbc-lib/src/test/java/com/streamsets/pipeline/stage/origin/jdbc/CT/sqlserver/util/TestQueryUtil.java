/**
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc.CT.sqlserver.util;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestQueryUtil {
  private final int maxBatchSize = 1;
  private final String tableName = "dbo.test";
  private final List<String> offsetColumns = ImmutableList.of(QueryUtil.SYS_CHANGE_VERSION, "pk1", "pk2");

  @Test
  public void testInitialQuery() throws Exception {
    Map<String, String> offsetMap = new HashMap<>();
    offsetMap.put(QueryUtil.SYS_CHANGE_VERSION, "0");
    String query = QueryUtil.buildQuery(offsetMap, maxBatchSize, tableName, offsetColumns, offsetMap);

    String expected = "SELECT TOP 1 * \nFROM " + tableName + " AS " + QueryUtil.TABLE_NAME +
        "\nRIGHT OUTER JOIN CHANGETABLE(CHANGES " + tableName + ", @synchronization_version) AS " +
        QueryUtil.CT_TABLE_NAME;

    Assert.assertTrue(StringUtils.contains(query, expected));
  }
}
