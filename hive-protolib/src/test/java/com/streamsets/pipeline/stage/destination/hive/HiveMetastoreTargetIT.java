/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.hive;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.BaseHiveIT;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.lib.hive.HiveMetastoreUtil;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveType;
import com.streamsets.pipeline.stage.lib.hive.typesupport.HiveTypeInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;

public class HiveMetastoreTargetIT extends BaseHiveIT {

  private static Logger LOG = LoggerFactory.getLogger(BaseHiveIT.class);

  HiveQueryExecutor hiveQueryExecutor;

  @Before
  public void setUpHiveQueryExecutor() {
    hiveQueryExecutor = new HiveQueryExecutor(getHiveJdbcUrl());
  }

  @Test
  public void testInitialization() throws Exception {
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());
  }

  @Test
  public void testCreateNonExistingTable() throws Exception {
    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder().build();

    TargetRunner runner = new TargetRunner.Builder(HiveMetastoreTarget.class, hiveTarget)
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    LinkedHashMap<String, HiveTypeInfo> columns = new LinkedHashMap<>();
    columns.put("name", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    LinkedHashMap<String, HiveTypeInfo> partitions = new LinkedHashMap<>();
    partitions.put("dt", HiveType.STRING.getSupport().generateHiveTypeInfoFromResultSet("STRING"));

    Field newTableField = HiveMetastoreUtil.newSchemaMetadataFieldBuilder(
      "default",
      "tbl",
      columns,
      partitions,
      true,
      "/user/hive/warehouse/tbl",
      HiveMetastoreUtil.generateAvroSchema(columns, "tbl")
    );

    Record record = RecordCreator.create();
    record.set(newTableField);
    Assert.assertTrue(HiveMetastoreUtil.isSchemaChangeRecord(record));

    runner.runWrite(ImmutableList.of(record));
    runner.runDestroy();

    Assert.assertTrue(hiveQueryExecutor.executeShowTableQuery("default.tbl"));
    Pair<LinkedHashMap<String, HiveTypeInfo>, LinkedHashMap<String, HiveTypeInfo>> tableStructure = hiveQueryExecutor.executeDescTableQuery("default.tbl");
    LinkedHashMap<String, HiveTypeInfo> retrievedColumns = tableStructure.getLeft();
    LinkedHashMap<String, HiveTypeInfo> retrievedPartitions = tableStructure.getRight();

    Assert.assertEquals(1, retrievedColumns.size());
    Assert.assertTrue(retrievedColumns.containsKey("name"));
    Assert.assertEquals(HiveType.STRING, retrievedColumns.get("name").getHiveType());

    Assert.assertEquals(1, retrievedPartitions.size());
    Assert.assertTrue(retrievedPartitions.containsKey("dt"));
    Assert.assertEquals(HiveType.STRING, retrievedPartitions.get("dt").getHiveType());
  }
}

