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
package com.streamsets.pipeline.stage.it;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.testing.ParametrizedUtils;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This tests tries various keywords and reserved words in database, table and column names
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
@Ignore
public class KeywordsInObjectNamesIT extends BaseHiveMetadataPropagationIT {

  private static Logger LOG = LoggerFactory.getLogger(ColdStartIT.class);

  @Parameterized.Parameters(name = "keyword({0})")
  public static Collection<Object[]> data() throws Exception {
    return ParametrizedUtils.toArrayOfArrays(
      // Various problematical strings

      // Hive and avro keywords and reserved words
      "table",
      "create",
      "date",
      "as",
      "year",
      "string",
      "default"
    );
  }

  private final String keyword;
  public KeywordsInObjectNamesIT(String str) {
    this.keyword = str;
  }

  @Test
  public void testKeywordInDbTableColumnName() throws  Exception {
    executeUpdate(Utils.format("CREATE DATABASE IF NOT EXISTS `{}`", keyword));
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .database(keyword)
      .table(keyword)
      .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
      .build();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put(keyword, Field.create("value"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      processRecords(processor, hiveTarget, ImmutableList.of(record));
    } catch(StageException se) {
      LOG.error("Processing exception", se);
      Assert.fail("Processing testing record unexpectedly failed: " + se.getMessage());
      throw se;
    }

    String fullTableName = Utils.format("`{}`.`{}`", keyword, keyword);
    assertTableExists(fullTableName);
    assertQueryResult(Utils.format("select * from {}", fullTableName), new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of(Utils.format("{}.{}", keyword, keyword), Types.VARCHAR),
            ImmutablePair.of(Utils.format("{}.dt", keyword), Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals("value", rs.getObject(1));
        Assert.assertFalse("Table tbl contains more then one row", rs.next());
      }
    });
  }

  @Test
  public void testKeywordInPartitionColumnName() throws  Exception {
    executeUpdate(Utils.format("CREATE DATABASE IF NOT EXISTS `{}`", keyword));
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .partitions(new PartitionConfigBuilder()
        .addPartition(keyword, HiveType.STRING, "value")
        .build())
      .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
      .build();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("col", Field.create("value"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      processRecords(processor, hiveTarget, ImmutableList.of(record));
    } catch(StageException se) {
      LOG.error("Processing exception", se);
      Assert.fail("Processing testing record unexpectedly failed: " + se.getMessage());
      throw se;
    }

    assertTableExists("default.tbl");
    assertQueryResult("select * from tbl", new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            ImmutablePair.of("tbl.col", Types.VARCHAR),
            ImmutablePair.of(Utils.format("tbl.{}", keyword), Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals("value", rs.getObject(1));
        Assert.assertEquals("value", rs.getObject(2));
        Assert.assertFalse("Table tbl contains more then one row", rs.next());
      }
    });
  }
}
