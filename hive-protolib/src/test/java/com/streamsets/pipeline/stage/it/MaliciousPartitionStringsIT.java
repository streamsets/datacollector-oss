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
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.processor.hive.Errors;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Collection;
import java.util.Arrays;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Goal is to verify all various possibly dangerous partition strings to make sure
 * that we either die gracefully or throw proper error.
 */
@RunWith(Parameterized.class)
@Ignore
public class MaliciousPartitionStringsIT extends BaseHiveMetadataPropagationIT {

  private static Logger LOG = LoggerFactory.getLogger(MaliciousPartitionStringsIT.class);

  @Parameterized.Parameters(name = "type({0}), expected({1})")
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][]{
        // Supported characters. They are not encoded in HDFS path
        {"-", Result.SUCCESS},
        {"_", Result.SUCCESS},
        {"$", Result.SUCCESS},
        {",", Result.SUCCESS},
        {"(", Result.SUCCESS},
        {")", Result.SUCCESS},
        {"&", Result.SUCCESS},
        {"@", Result.SUCCESS},
        {"!", Result.SUCCESS},
        {".", Result.SUCCESS},
        {"|", Result.SUCCESS},
        {"~", Result.SUCCESS},
        {"`", Result.SUCCESS},
        // Unsupported characters. Processor will send the record to error record.
        {"\\", Result.ERROR_RECORD},
        {"'", Result.ERROR_RECORD},
        {"[", Result.ERROR_RECORD},
        {"]", Result.ERROR_RECORD},
        {"/", Result.ERROR_RECORD},
        {"*", Result.ERROR_RECORD},
        {"?", Result.ERROR_RECORD},
        {"\"", Result.ERROR_RECORD},
        {"%", Result.ERROR_RECORD},
        {"=", Result.ERROR_RECORD},
        {"^", Result.ERROR_RECORD},
    });
  }

  private final String partitionValue;
  private final Result expected;

  public MaliciousPartitionStringsIT(String partitionValue, Result expected) {
    this.partitionValue = partitionValue;
    this.expected = expected;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testType() throws  Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .partitions(new PartitionConfigBuilder()
        .addPartition("part", HiveType.STRING, partitionValue)
        .build())
      .build();
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
      .build();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("col", Field.create("value"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      processRecords(processor, hiveTarget, ImmutableList.of(record), OnRecordError.TO_ERROR);
    } catch(Exception e) {
      LOG.error("Received Exception with", e);
      // Currently we don't have characters that we throw StageException.
      fail(Utils.format("Partition value '{}' failed with {}", partitionValue, e.getMessage()));
    }

    if (expected == Result.ERROR_RECORD){
      // Check the contents of the error record
      List<Record> errorRecord  = getErrorRecord(Stage.METADATA_PROCESSOR);
      Assert.assertEquals(1, errorRecord.size());
      Assert.assertEquals(
          Errors.HIVE_METADATA_10.name(),
          errorRecord.get(0).getHeader().getErrorCode()
      );

    } else if (expected == Result.SUCCESS) {
      assertTableExists("default.tbl");
      assertQueryResult("select * from tbl", new QueryValidator() {
        @Override
        public void validateResultSet(ResultSet rs) throws Exception {
          assertResultSetStructure(rs,
              ImmutablePair.of("tbl.col", Types.VARCHAR),
              ImmutablePair.of("tbl.part", Types.VARCHAR)
          );

          Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
          Assert.assertEquals("value", rs.getString(1));
          Assert.assertEquals(partitionValue, rs.getString(2));
          Assert.assertFalse("Table tbl contains more then one row", rs.next());
        }
      });
    }
  }
}
