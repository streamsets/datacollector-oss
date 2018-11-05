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
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.UtilsProvider;
import com.streamsets.pipeline.lib.jdbc.multithread.util.OffsetQueryUtil;
import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.UUID;

@RunWith(Parameterized.class)
public class TestInvalidStartOffset {

  private final int sqlType;
  private final TableContextUtil tableContextUtil;

  public TestInvalidStartOffset(int sqlType) {
    this.sqlType = sqlType;
    this.tableContextUtil = UtilsProvider.getTableContextUtil();
  }

  @Parameterized.Parameters(name = "SQL Type : {0}")
  public static Object[] data() throws Exception {
    Collection<Integer> typesToCheck = new ArrayList<>(OffsetQueryUtil.SQL_TYPE_TO_FIELD_TYPE.keySet());
    typesToCheck.removeAll(OffsetQueryUtil.UNSUPPORTED_OFFSET_SQL_TYPES);
    //Don't have to check string columns
    typesToCheck.removeAll(
        Arrays.asList(
            Types.CHAR,
            Types.VARCHAR,
            Types.NCHAR,
            Types.NVARCHAR,
            Types.LONGNVARCHAR,
            Types.LONGVARCHAR
        )
    );
    return typesToCheck.toArray();
  }

  @BeforeClass
  public static void setUpClass() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Test
  public void testCheckErrorOnInvalidStartOffset() throws Exception {
    String randomStringValue = UUID.randomUUID().toString();
    try {
      tableContextUtil.checkForInvalidInitialOffsetValues(
          Mockito.mock(PushSource.Context.class),
          new LinkedList<Stage.ConfigIssue>(),
          "",
          new LinkedHashMap<>(ImmutableMap.of("column", sqlType)),
          ImmutableMap.of("column", randomStringValue)
      );
      Assert.fail("Invalid start offset should have failed");
    } catch (StageException e) {
      Assert.assertEquals(JdbcErrors.JDBC_72.name(), e.getErrorCode().getCode());
      Assert.assertTrue(e.getMessage().contains("column - " + randomStringValue));
    }
  }

}
