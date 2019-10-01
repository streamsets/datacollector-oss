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
package com.streamsets.pipeline.stage.destination.http;

import com.streamsets.datacollector.event.json.SDCMetricsJson;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.testing.NetworkUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseHttpTargetTest extends JerseyTest {

  private static List<SDCMetricsJson> records = new ArrayList<>();

  protected abstract HttpTarget createHttpTarget();

  @Override
  protected DeploymentContext configureDeployment() {
    forceSet(TestProperties.CONTAINER_PORT, String.valueOf(NetworkUtils.getRandomPort()));
    ResourceConfig resourceConfig = new ResourceConfig(MockServer.class);
    return ServletDeploymentContext.forServlet(
      new ServletContainer(resourceConfig)
    ).build();
  }

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
    HttpTarget httpTarget = createHttpTarget();
    TargetRunner targetRunner = new TargetRunner.Builder(HttpDTarget.class, httpTarget)
      .build();
    targetRunner.runInit();
    targetRunner.runWrite(createRecords());

    for (int i = 0; i < 20; i++) {
      SDCMetricsJson record = records.get(i);
      Assert.assertEquals(String.valueOf(i), record.getSdcId());
      Assert.assertEquals("a", record.getMetadata().entrySet().iterator().next().getKey());
      Assert.assertEquals("b", record.getMetadata().entrySet().iterator().next().getValue());
      Assert.assertEquals("masterSDC", record.getMasterSdcId());
      Assert.assertEquals("x", record.getMetadata().get(HttpTarget.DPM_PIPELINE_COMMIT_ID));
      Assert.assertEquals("y", record.getMetadata().get(HttpTarget.DPM_JOB_ID));
      Assert.assertTrue(record.isAggregated());
    }
  }

  private List<Record> createRecords() throws IOException {
    List<Record> list = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Map<String, Object> m = new HashMap<>();
      m.put("a", "b");
      Record record = AggregatorUtil.createMetricJsonRecord(
          String.valueOf(i),
          "masterSDC",
          m,
          true,
          true,
          false,
          "{}"
      );
      list.add(record);
    }
    return list;
  }

  @Override
  protected final TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return new GrizzlyWebTestContainerFactory();
  }

}
