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
package com.streamsets.pipeline.stage.processor.httprouter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHttpRouterProcessor {

  private List<HttpRouterLaneConfig> createRouterConfigs(String ... args) {
    List<HttpRouterLaneConfig> routerLaneConfigs = new ArrayList<>();
    if (args.length % 3 != 0) {
      Assert.fail("Router Configs must come in triplets");
    }
    for (int i = 0; i < args.length; i = i + 3) {
      HttpRouterLaneConfig routerLaneConfig = new HttpRouterLaneConfig();
      routerLaneConfig.outputLane = args[i];
      routerLaneConfig.httpMethod = HttpMethod.valueOf(args[i+1]);
      routerLaneConfig.pathParam = args[i+2];
      routerLaneConfigs.add(routerLaneConfig);
    }
    return routerLaneConfigs;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testValidation() throws Exception {
    List<HttpRouterLaneConfig> routerLaneConfigs = createRouterConfigs();
    Processor httpRouter = new HttpRouterProcessor(routerLaneConfigs);
    ProcessorRunner runner = new ProcessorRunner.Builder(HttpRouterDProcessor.class, httpRouter)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("HTTP_ROUTER_00"));


    routerLaneConfigs = createRouterConfigs("a","GET", "/route1", "b", "POST", "/route2");
    httpRouter = new HttpRouterProcessor(routerLaneConfigs);
    runner = new ProcessorRunner.Builder(HttpRouterDProcessor.class, httpRouter)
        .setOnRecordError(OnRecordError.DISCARD)
        .addOutputLane("a")
        .build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains("HTTP_ROUTER_01"));
  }

  @Test
  public void testInit() throws Exception {
    Map<String, Object> constants = new HashMap<>();
    constants.put("x", "false");
    ProcessorRunner runner = new ProcessorRunner.Builder(HttpRouterDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("routerLaneConfigs", createRouterConfigs("a","GET", "/route1", "b", "POST", "/route2"))
        .addConstants(constants)
        .addOutputLane("a")
        .addOutputLane("b")
        .build();
    runner.runInit();
  }

  @Test
  public void testProcess() throws Exception {
    ProcessorRunner runner = new ProcessorRunner.Builder(HttpRouterDProcessor.class)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addConfiguration("routerLaneConfigs", createRouterConfigs("a","GET", "/route1", "b", "POST", "/route2"))
        .addOutputLane("a")
        .addOutputLane("b")
        .build();

    runner.runInit();
    try {
      Record r0 = RecordCreator.create();
      r0.set(Field.create(0));
      r0.getHeader().setAttribute("method", "GET");
      r0.getHeader().setAttribute("path", "/route1");


      Record r1 = RecordCreator.create();
      r1.set(Field.create(1));
      r1.getHeader().setAttribute("method", "GET");
      r1.getHeader().setAttribute("path", "/route1");

      Record r2 = RecordCreator.create();
      r2.set(Field.create(2));
      r2.getHeader().setAttribute("method", "POST");
      r2.getHeader().setAttribute("path", "/route2");

      Record r3 = RecordCreator.create();
      r3.set(Field.create(3));
      r3.getHeader().setAttribute("method", "POST");
      r3.getHeader().setAttribute("path", "/route2");

      Record r4 = RecordCreator.create();
      r4.set(Field.create(3));
      r4.getHeader().setAttribute("method", "PUT");
      r4.getHeader().setAttribute("path", "/route2");


      List<Record> input = ImmutableList.of(r0, r1, r2, r3, r4);
      StageRunner.Output output = runner.runProcess(input);

      Assert.assertEquals(ImmutableSet.of("a", "b"), output.getRecords().keySet());

      Assert.assertEquals(2, output.getRecords().get("a").size());
      Assert.assertEquals(0, output.getRecords().get("a").get(0).get().getValueAsInteger());

      Assert.assertEquals(2, output.getRecords().get("b").size());
      Assert.assertEquals(2, output.getRecords().get("b").get(0).get().getValueAsInteger());

      Assert.assertEquals(1, runner.getErrorRecords().size());


    } finally {
      runner.runDestroy();
    }
  }

}
