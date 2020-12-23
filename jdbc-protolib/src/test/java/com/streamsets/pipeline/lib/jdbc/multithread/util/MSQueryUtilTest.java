/*
 * Copyright 2019 StreamSets Inc.
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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MSQueryUtilTest {

  Map<String, String> offsetMap = new HashMap<>();
  int fetchSize;
  String tableName;
  Collection<String> offsetColumns = new ArrayList<>();
  boolean includeJoin = false;
  long offset = 0;

  @Before
  public void setup() {
    offsetMap.put("SYS_CHANGE_VERSION", "69");
    offsetMap.put("id", "3");
    offsetMap.put("SYS_CHANGE_OPERATION", "I");

    tableName = "foo";

    offsetColumns.add("id");
    offsetColumns.add("SYS_CHANGE_VERSION");
    includeJoin = false;
    offset = 0;
  }

  @Test
  public void testBuildQueryNonZeroFetch() {
    /*
        FetchSize is used as a form of "pagination" meaning when we query,
        we expect to receive back more than a batch size.

        Hence expect to query subset of changes and order them by ...
     */
    fetchSize = 1000;

    String result = MSQueryUtil.buildQuery(offsetMap, fetchSize, tableName, offsetColumns, includeJoin, offset);
    String expected = "SELECT * FROM CHANGETABLE(CHANGES foo, 69) AS CT WHERE (CT.[id] > '3'  AND CT.SYS_CHANGE_VERSION" +
        " " +
        "= 69 ) OR (CT.SYS_CHANGE_VERSION > '69' ) ORDER BY SYS_CHANGE_VERSION, CT.[id]";
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testBuildQueryZeroFetch() {
    /*
        If FetchSize is zero, PreparedStatement uses the DBs default value. We then don't sort etc, and
        take entire resultSet to process.
     */
    fetchSize = 0;

    String result = MSQueryUtil.buildQuery(offsetMap, fetchSize, tableName, offsetColumns, includeJoin, offset);
    String expected = "SELECT * FROM CHANGETABLE(CHANGES foo, 69) AS CT WHERE CT.SYS_CHANGE_VERSION > '69'";
    Assert.assertEquals(expected, result);

  }
}
