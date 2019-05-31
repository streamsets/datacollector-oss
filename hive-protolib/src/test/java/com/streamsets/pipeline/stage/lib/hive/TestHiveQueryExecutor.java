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

package com.streamsets.pipeline.stage.lib.hive;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.LinkedHashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHiveQueryExecutor {

  private static final String RESULT_SET_COL_NAME = "col_name";
  private static final String RESULT_SET_DATA_TYPE = "data_type";

  private static final String MOCK_TYPE = "int";

  @Test
  public void testExtractTypeInfoHive2() throws Exception {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(false);

    when(resultSet.getString(RESULT_SET_COL_NAME)).thenReturn("id").thenReturn("object_id").thenReturn("").thenReturn(
        "#").thenReturn("#").thenReturn("partition");

    when(resultSet.getString(RESULT_SET_DATA_TYPE)).thenReturn(MOCK_TYPE);

    HiveTypeInfo hiveTypeInfo = HiveType.prefixMatch(MOCK_TYPE)
                                        .getSupport()
                                        .generateHiveTypeInfoFromResultSet(MOCK_TYPE);

    LinkedHashMap<String, HiveTypeInfo> columnTypeInfo = new LinkedHashMap<>();
    columnTypeInfo.put("id", hiveTypeInfo);
    columnTypeInfo.put("object_id", hiveTypeInfo);

    LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo = new LinkedHashMap<>();
    partitionTypeInfo.put("partition", hiveTypeInfo);

    HiveQueryExecutor hiveQueryExecutor = new HiveQueryExecutor(mock(HiveConfigBean.class), mock(Stage.Context.class));

    Pair<LinkedHashMap<String, HiveTypeInfo>, LinkedHashMap<String, HiveTypeInfo>>
        result
        = hiveQueryExecutor.extractTypeInfo(resultSet);

    Assert.assertEquals(columnTypeInfo, result.getKey());
    Assert.assertEquals(partitionTypeInfo, result.getValue());
  }

  @Test
  public void testExtractTypeInfoHive3() throws Exception {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(false);

    when(resultSet.getString(RESULT_SET_COL_NAME)).thenReturn("id").thenReturn("object_id").thenReturn("").thenReturn(
        "#").thenReturn("#").thenReturn("").thenReturn("partition");

    when(resultSet.getString(RESULT_SET_DATA_TYPE)).thenReturn(MOCK_TYPE);

    HiveTypeInfo hiveTypeInfo = HiveType.prefixMatch(MOCK_TYPE)
                                        .getSupport()
                                        .generateHiveTypeInfoFromResultSet(MOCK_TYPE);

    LinkedHashMap<String, HiveTypeInfo> columnTypeInfo = new LinkedHashMap<>();
    columnTypeInfo.put("id", hiveTypeInfo);
    columnTypeInfo.put("object_id", hiveTypeInfo);

    LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo = new LinkedHashMap<>();
    partitionTypeInfo.put("partition", hiveTypeInfo);

    HiveQueryExecutor hiveQueryExecutor = new HiveQueryExecutor(mock(HiveConfigBean.class), mock(Stage.Context.class));

    Pair<LinkedHashMap<String, HiveTypeInfo>, LinkedHashMap<String, HiveTypeInfo>>
        result
        = hiveQueryExecutor.extractTypeInfo(resultSet);

    Assert.assertEquals(columnTypeInfo, result.getKey());
    Assert.assertEquals(partitionTypeInfo, result.getValue());
  }

  @Test
  public void testExtractTypeInfoHive_noPartitionInfo() throws Exception {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(true)
                          .thenReturn(false);

    when(resultSet.getString(RESULT_SET_COL_NAME)).thenReturn("id").thenReturn("object_id").thenReturn("").thenReturn(
        "#").thenReturn("#");

    when(resultSet.getString(RESULT_SET_DATA_TYPE)).thenReturn(MOCK_TYPE);

    HiveTypeInfo hiveTypeInfo = HiveType.prefixMatch(MOCK_TYPE)
                                        .getSupport()
                                        .generateHiveTypeInfoFromResultSet(MOCK_TYPE);

    LinkedHashMap<String, HiveTypeInfo> columnTypeInfo = new LinkedHashMap<>();
    columnTypeInfo.put("id", hiveTypeInfo);
    columnTypeInfo.put("object_id", hiveTypeInfo);

    LinkedHashMap<String, HiveTypeInfo> partitionTypeInfo = new LinkedHashMap<>();

    HiveQueryExecutor hiveQueryExecutor = new HiveQueryExecutor(mock(HiveConfigBean.class), mock(Stage.Context.class));

    Pair<LinkedHashMap<String, HiveTypeInfo>, LinkedHashMap<String, HiveTypeInfo>>
        result
        = hiveQueryExecutor.extractTypeInfo(resultSet);

    Assert.assertEquals(columnTypeInfo, result.getKey());
    Assert.assertEquals(partitionTypeInfo, result.getValue());
  }
}
