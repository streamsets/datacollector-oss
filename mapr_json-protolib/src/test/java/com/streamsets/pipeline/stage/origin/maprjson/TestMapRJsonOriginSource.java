/**
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.maprjson;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

@Category(SingleForkNoReuseTest.class)
public class TestMapRJsonOriginSource {

  private void basicConfiguration(MapRJsonOriginConfigBean conf) {

    conf.startValue = "";
    conf.tableName = "testTable";

  }

  @Test
  public void testTableName() {
    MapRJsonOriginConfigBean conf = new MapRJsonOriginConfigBean();
    basicConfiguration(conf);

    MapRJsonOriginSource mapr = new MapRJsonOriginSource(conf);
    assertEquals("testTable", conf.tableName);

  }

  @Test
  public void testTableDoesNotExist() {

    MapRJsonOriginConfigBean conf = new MapRJsonOriginConfigBean();
    basicConfiguration(conf);

    MapRJsonOriginSource mapr = new MapRJsonOriginSource(conf);

    SourceRunner runner = new SourceRunner.Builder(MapRJsonOriginSource.class, mapr)
        .addOutputLane("lane")
        .setOnRecordError(OnRecordError.DISCARD)
        .build();

    try {
      List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
      assertEquals(1, issues.size());
      assertTrue(issues.get(0).toString().contains("MAPR_JSON_ORIGIN_06"));
    } catch(Exception e) {
      e.printStackTrace();
    }

  }

}
