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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.http;

import com.streamsets.datacollector.restapi.bean.SDCMetricsJson;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHttpTarget extends JerseyTest {

  private static final String URL = "http://localhost:9998/send/records";
  private static List<SDCMetricsJson> records = new ArrayList<>();

  @Path("/send/records")
  @Consumes(MediaType.APPLICATION_JSON)
  public static class MockServer {

    @POST
    public Response saveMetrics(List<SDCMetricsJson> json) throws IOException {
      records.addAll(json);
      return Response.ok().build();
    }
  }

  @Test
  public void testHttpTarget() throws StageException, IOException {
    HttpTarget httpTarget = new HttpTarget(URL, "token", "sdc", "x", "y", 0);
    TargetRunner targetRunner = new TargetRunner.Builder(HttpTarget.class, httpTarget)
      .build();
    targetRunner.runInit();
    targetRunner.runWrite(createRecords());

    for (int i = 0; i < 20; i++) {
      SDCMetricsJson record = records.get(i);
      Assert.assertEquals(String.valueOf(i), record.getSdcId());
      Assert.assertEquals("a", record.getMetadata().entrySet().iterator().next().getKey());
      Assert.assertEquals("b", record.getMetadata().entrySet().iterator().next().getValue());
      Assert.assertEquals("x", record.getMetadata().get(HttpTarget.DPM_PIPELINE_COMMIT_ID));
      Assert.assertEquals("y", record.getMetadata().get(HttpTarget.DPM_JOB_ID));
      Assert.assertTrue(record.isAggregated());
    }
  }

  public List<Record> createRecords() throws IOException {
    List<Record> list = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Map<String, String> m = new HashMap<>();
      m.put("a", "b");
      Record record = AggregatorUtil.createMetricJsonRecord(String.valueOf(i), m, true, "{}");
      list.add(record);
    }
    return list;
  }

  @Override
  protected Application configure() {
    return new ResourceConfig(MockServer.class);
  }

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return new GrizzlyWebTestContainerFactory();
  }

  @Override
  protected DeploymentContext configureDeployment() {
    return ServletDeploymentContext.forServlet(
      new ServletContainer(new ResourceConfig(MockServer.class))
    ).build();
  }

}
