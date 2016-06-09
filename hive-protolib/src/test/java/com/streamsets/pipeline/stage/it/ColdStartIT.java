/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.HiveMetastoreTargetBuilder;
import com.streamsets.pipeline.stage.destination.hive.HiveMetastoreTarget;
import com.streamsets.pipeline.stage.processor.hive.HiveMetadataProcessor;
import com.streamsets.pipeline.stage.HiveMetadataProcessorBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import com.streamsets.pipeline.stage.ParametrizedUtils;

/**
 * Run all combination of old/new way of creating tables with internal/external for case when the table in HMS
 * doesn't exists before starting the pipeline.
 */
@RunWith(Parameterized.class)
public class ColdStartIT extends BaseHiveMetadataPropagationIT {

  private static Logger LOG = LoggerFactory.getLogger(ColdStartIT.class);

  @Parameterized.Parameters(name = "db({0}),storedAsAvro({1}),external({2})")
  public static Collection<Object[]> data() {
    return ParametrizedUtils.crossProduct(
        new String[] {"default", "custom"},
        new Boolean[] {true, false},
        new Boolean[] {true, false}
    );
  }

  private String database;
  private boolean storedAsAvro;
  private boolean external;
  public ColdStartIT(String database, boolean storedAsAvro, boolean external) {
    this.database = database;
    this.storedAsAvro = storedAsAvro;
    this.external = external;
  }


  @Test
  public void testColdStart() throws  Exception {
    LOG.info(Utils.format("Starting cold start with database({}), storedAsAvro({}) and external({})", database, storedAsAvro, external));
    executeUpdate(Utils.format("create database if not exists {}", database));

    HiveMetadataProcessor processor = new HiveMetadataProcessorBuilder()
      .database(database)
      .tablePathTemplate("/user/hive/warehouse")
      .partitionPathTemplate("dt=super-secret")
      .external(external)
      .build();

    HiveMetastoreTarget hiveTarget = new HiveMetastoreTargetBuilder()
        .useAsAvro(storedAsAvro)
        .build();

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("name", Field.create(Field.Type.STRING, "StreamSets"));
    Record record = RecordCreator.create("s", "s:1");
    record.set(Field.create(map));

    processRecords(processor, hiveTarget, ImmutableList.of(record));

    assertTableExists(Utils.format("{}.tbl", database));
    assertQueryResult(Utils.format("select * from {}.tbl", database), new QueryValidator() {
      @Override
      public void validateResultSet(ResultSet rs) throws Exception {
        assertResultSetStructure(rs,
            new ImmutablePair("tbl.name", Types.VARCHAR),
            new ImmutablePair("tbl.dt", Types.VARCHAR)
        );

        Assert.assertTrue("Table tbl doesn't contain any rows", rs.next());
        Assert.assertEquals("StreamSets", rs.getString(1));
        Assert.assertFalse("Table tbl contains more then one row", rs.next());
      }
    });
  }
}
