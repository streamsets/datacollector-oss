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
package com.streamsets.pipeline.stage.processor.hive;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import com.streamsets.testing.ParametrizedUtils;
import com.streamsets.pipeline.stage.PartitionConfigBuilder;
import com.streamsets.pipeline.stage.it.ColdStartIT;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This tests tries various disallowed characters in table, column and partition names.
 */
@RunWith(Parameterized.class)
public class MaliciousStringsInObjectNamesIT extends BaseHiveIT {

  private static Logger LOG = LoggerFactory.getLogger(ColdStartIT.class);

  @Parameterized.Parameters(name = "str({0})")
  public static Collection<Object[]> data() throws Exception {
    return ParametrizedUtils.toArrayOfArrays(
      "tbl#tbl",
      "tbl-tbl",
      "tbl$tbl",
      "tbl.tbl"
    );
  }

  private final String keyword;

  public MaliciousStringsInObjectNamesIT(String str) {
    this.keyword = str;
  }

  @Test
  public void testInTable() throws  Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .table(keyword)
      .build();

    ProcessorRunner procesorRunner = new ProcessorRunner.Builder(HiveMetadataDProcessor.class, processor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .addOutputLane("hdfs")
      .addOutputLane("hive")
      .build();
    procesorRunner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("col", Field.create("value"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      procesorRunner.runProcess(ImmutableList.of(record));
      Assert.fail("Expected exception!");
    } catch(OnRecordErrorException e) {
      Assert.assertEquals(Errors.HIVE_METADATA_03, e.getErrorCode());
    }
  }

  @Test
  public void testInColumnName() throws  Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .build();

    ProcessorRunner procesorRunner = new ProcessorRunner.Builder(HiveMetadataDProcessor.class, processor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .addOutputLane("hdfs")
      .addOutputLane("hive")
      .build();
    procesorRunner.runInit();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put(keyword, Field.create("value"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    try {
      procesorRunner.runProcess(ImmutableList.of(record));
    } catch(OnRecordErrorException e) {
      Assert.assertEquals(com.streamsets.pipeline.stage.lib.hive.Errors.HIVE_30, e.getErrorCode());
    }
  }

  @Test
  public void testInPartitionName() throws  Exception {
    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .partitions(new PartitionConfigBuilder()
        .addPartition(keyword, HiveType.STRING, "value")
        .build())
      .build();

    ProcessorRunner procesorRunner = new ProcessorRunner.Builder(HiveMetadataDProcessor.class, processor)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .addOutputLane("hdfs")
      .addOutputLane("hive")
      .build();

    List<Stage.ConfigIssue> issues = procesorRunner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Stage.ConfigIssue issue = issues.get(0);
    Assert.assertTrue(
      Utils.format("Didn't find required error string in: {}", issue.toString()),
      issue.toString().contains(Utils.format("HIVE_METADATA_03 - Failed validation on Partition Configuration : Invalid character for Hive '{}'", keyword))
    );
  }
}
