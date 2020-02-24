/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.oracle.cdc;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaAndTable;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class TestOracleCDCSource {

  @Test
  public void testGetListOfSchemasAndTables(){
    List<SchemaAndTable> tables = ImmutableList.of(
        new SchemaAndTable("SYS", "TEST1"),
        new SchemaAndTable("SYS", "TEST2")
    );
    OracleCDCConfigBean configBean = new OracleCDCConfigBean();
    configBean.dictionary = DictionaryValues.DICT_FROM_ONLINE_CATALOG;
    OracleCDCSource source = new OracleCDCSource(null, configBean);
    String actual = source.getListOfSchemasAndTables(tables);
    Assert.assertEquals("( (SEG_OWNER='SYS' AND (TABLE_NAME IN ('TEST1','TEST2'))) )", actual);
  }

  @Test
  public void testGetListOfSchemasAndTablesBig(){
    List<String> listOfTables = new ArrayList<>(1010);
    List<SchemaAndTable> schemaAndTables = new ArrayList<>(1010);
    for (int i = 0; i < 1010; i++) {
      String tableName = RandomStringUtils.randomAlphanumeric(5);
      listOfTables.add(Utils.format("'{}'",tableName));
      schemaAndTables.add(new SchemaAndTable("SYS", tableName));
    }

    OracleCDCConfigBean configBean = new OracleCDCConfigBean();
    configBean.dictionary = DictionaryValues.DICT_FROM_ONLINE_CATALOG;
    OracleCDCSource source = new OracleCDCSource(null, configBean);

    String actual = source.getListOfSchemasAndTables(schemaAndTables);
    String expected = Utils.format(
        "( (SEG_OWNER='SYS' AND (TABLE_NAME IN ({}) OR TABLE_NAME IN ({}))) )",
        String.join(",", listOfTables.subList(0,1000)),
        String.join(",", listOfTables.subList(1000,1010))
    );
    Assert.assertEquals(expected, actual);
  }
}
