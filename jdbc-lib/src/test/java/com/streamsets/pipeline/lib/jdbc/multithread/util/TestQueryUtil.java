/*
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

package com.streamsets.pipeline.lib.jdbc.multithread.util;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestQueryUtil {
  private final int maxBatchSize = 1;
  private final String tableName = "dbo.test";
  private final List<String> offsetColumns = ImmutableList.of(MSQueryUtil.SYS_CHANGE_VERSION, "pk1", "pk2");

  @Test
  public void testInitialQuery() {
    Map<String, String> offsetMap = new HashMap<>();
    offsetMap.put(MSQueryUtil.SYS_CHANGE_VERSION, "0");
    final boolean includeJoin = true;

    String query = MSQueryUtil.buildQuery(offsetMap, maxBatchSize, tableName, offsetColumns, offsetMap, includeJoin, 0);

    String expected = "SELECT * \nFROM " + tableName + " AS " + MSQueryUtil.TABLE_NAME +
        "\nRIGHT OUTER JOIN CHANGETABLE(CHANGES " + tableName + ", @synchronization_version) AS " +
        MSQueryUtil.CT_TABLE_NAME;

    Assert.assertTrue(StringUtils.contains(query, expected));
  }

  @Test
  public void testMSQLCTQuery() {
    Map<String, String> offsetMap = new HashMap<>();
    offsetMap.put(MSQueryUtil.SYS_CHANGE_VERSION, "0");
    offsetMap.put("pk1", "1");
    offsetMap.put("pk2", "2");
    final boolean includeJoin = false;

    String query = MSQueryUtil.buildQuery(offsetMap, maxBatchSize, tableName, offsetColumns, offsetMap, includeJoin, 0);

    String expected = "SELECT * FROM CHANGETABLE(CHANGES dbo.test, 0) AS CT " +
        "WHERE (CT.pk1 > '1'  AND CT.pk2 > '2'  AND CT.SYS_CHANGE_VERSION = 0 ) OR (CT.SYS_CHANGE_VERSION > '0' )    " +
        "ORDER BY SYS_CHANGE_VERSION, CT.pk1, CT.pk2 ";

    Assert.assertTrue(StringUtils.contains(query, expected));
  }

  @Test
  public void testMSQLCTQueryIncludeJoin() {
    Map<String, String> offsetMap = new HashMap<>();
    offsetMap.put(MSQueryUtil.SYS_CHANGE_VERSION, "0");
    offsetMap.put("pk1", "1");
    offsetMap.put("pk2", "2");
    final boolean includeJoin = true;

    String query = MSQueryUtil.buildQuery(offsetMap, maxBatchSize, tableName, offsetColumns, offsetMap, includeJoin, 0);

    String expected = "SELECT * \n" +
        "FROM dbo.test AS P\n" +
        "RIGHT OUTER JOIN CHANGETABLE(CHANGES dbo.test, 0) AS CT\n" +
        " ON CT.pk1 = P.pk1  AND CT.pk2 = P.pk2 \n"+
        "WHERE (CT.pk1 > '1'  AND CT.pk2 > '2'  AND CT.SYS_CHANGE_VERSION = 0 ) OR (CT.SYS_CHANGE_VERSION > '0' )  \n" +
        " ORDER BY SYS_CHANGE_VERSION, CT.pk1, CT.pk2 ";

    Assert.assertTrue(StringUtils.contains(query, expected));
  }

  @Test
  public void testMSQLCDCQueryWithNoStartOffset() {
    Map<String, String> offsetMap = new HashMap<>();
    offsetMap.put(MSQueryUtil.SYS_CHANGE_VERSION, "0");
    offsetMap.put("__$start_lsn", "1");
    offsetMap.put("__$seqval", "2");
    final boolean allowLateTable = false;
    final boolean enableSchemaChanges = false;


    Map<String, String> startOffset = new HashMap<>();


    String query = MSQueryUtil.buildCDCQuery(offsetMap, tableName, startOffset, allowLateTable, enableSchemaChanges);

    String expected = "SELECT * FROM dbo.test WHERE ((__$start_lsn = CAST(0x1 AS BINARY(10)) ) AND (__$seqval > CAST" +
        "(0x2 AS BINARY(10)) ) ) OR (__$start_lsn > CAST(0x1 AS BINARY(10)) )   ORDER BY __$start_lsn, __$seqval";

    Assert.assertTrue(StringUtils.contains(query, expected));
  }

  @Test
  public void testMSSQLCTValidateQuery() {
    final String schema = "sa";
    final String table = "name";
    String query = MSQueryUtil.getMinVersion(schema, table);

    final String expected = "SELECT min_valid_version \n"
        + "FROM sys.change_tracking_tables t\n"
        + "WHERE t.object_id = OBJECT_ID('sa.name')";

    Assert.assertTrue(StringUtils.contains(query, expected));
  }
}
