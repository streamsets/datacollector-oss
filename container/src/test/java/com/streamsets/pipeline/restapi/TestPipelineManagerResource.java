/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.restapi.bean.PipelineStateJson;
import com.streamsets.pipeline.restapi.bean.SnapshotStatusJson;
import com.streamsets.pipeline.restapi.bean.StateJson;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;

public class TestPipelineManagerResource extends JerseyTest {
  
  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";

  @Test
  public void testGetStatusAPI() {
    PipelineStateJson state = target("/v1/pipeline/status").request().get(PipelineStateJson.class);
    Assert.assertNotNull(state);
    Assert.assertEquals(StateJson.STOPPED, state.getState());
    Assert.assertEquals("Pipeline is not running", state.getMessage());
  }

  @Test
  public void testStartPipelineAPI() {
    Response r = target("/v1/pipeline/start").queryParam("name", PIPELINE_NAME).queryParam("rev", PIPELINE_REV)
        .request().post(null);
    Assert.assertNotNull(r);
    PipelineStateJson state = r.readEntity(PipelineStateJson.class);
    Assert.assertNotNull(state);
    Assert.assertEquals(StateJson.RUNNING, state.getState());
    Assert.assertEquals("The pipeline is now running", state.getMessage());
  }

  @Test
  public void testStopPipelineAPI() {
    Response r = target("/v1/pipeline/stop").queryParam("rev", PIPELINE_REV)
        .request().post(null);
    Assert.assertNotNull(r);
    PipelineStateJson state = r.readEntity(PipelineStateJson.class);
    Assert.assertNotNull(state);
    Assert.assertEquals(StateJson.STOPPED, state.getState());
    Assert.assertEquals("The pipeline is not running", state.getMessage());
  }

  @Test
  public void testGetSnapshotsInfoAPI() throws IOException {
    Response r = target("/v1/pipeline/snapshots").request().get();
    Assert.assertNotNull(r);
  }

  @Test
   public void testCaptureSnapshotAPI() {
    Entity<String> stringEntity = Entity.entity("", MediaType.APPLICATION_JSON);
    Response r = target("/v1/pipeline/snapshots/snapshot1").request().put(stringEntity);
    Assert.assertNotNull(r);
  }

  @Test
  public void testGetSnapshotAPI() throws IOException {
    Response r = target("/v1/pipeline/snapshots/myPipeline/snapshot").request().get();
    Assert.assertNotNull(r);

    StringWriter writer = new StringWriter();
    IOUtils.copy((InputStream)r.getEntity(), writer);
    Assert.assertEquals("{\"offset\" : \"abc\"}", writer.toString());

  }

  @Test
  public void testDeleteSnapshotAPI() throws IOException {
    Response r = target("/v1/pipeline/snapshots/myPipeline/snapshot").request().delete();
    Assert.assertNotNull(r);
  }

  @Test
  public void testSnapshotStatusAPI() throws IOException {
    Response r = target("/v1/pipeline/snapshots/snapshot").queryParam("get","status").request().get();
    Assert.assertNotNull(r);

    SnapshotStatusJson s = r.readEntity(SnapshotStatusJson.class);

    Assert.assertEquals(false, s.isExists());
    Assert.assertEquals(true, s.isSnapshotInProgress());
  }

  @Test
  public void testGetMetricsAPI() {
    Response r = target("/v1/pipeline/metrics").request().get();
    Assert.assertNotNull(r);

    MetricRegistry m = r.readEntity(MetricRegistry.class);
    Assert.assertNotNull(m);
  }

  //List<PipelineState> pipelineStates
  @Test
  public void testGetHistoryAPI() {
    Response r = target("/v1/pipeline/history/myPipeline").request().get();
    Assert.assertNotNull(r);
    List<PipelineStateJson> pipelineStateJsons = r.readEntity(new GenericType<List<PipelineStateJson>>() {});
    Assert.assertEquals(3, pipelineStateJsons.size());
  }

  @Test
  public void testDeleteErrorRecordsAPI() throws IOException {
    Response r = target("/v1/pipeline/errors/myPipeline").request().delete();
    Assert.assertNotNull(r);

  }

  @Test
  public void testGetErrorsAPI() throws IOException {
    Response r = target("/v1/pipeline/errors/myPipeline").request().get();
    Assert.assertNotNull(r);

  }

  @Test
  public void testGetErrorRecordsAPI() throws IOException {
    Response r = target("/v1/pipeline/errorRecords").queryParam("stageInstanceName", "myProcessorStage").request().get();
    Assert.assertNotNull(r);

  }

  @Test
   public void testGetErrorMessagesAPI() throws IOException {
    Response r = target("/v1/pipeline/errorMessages").queryParam("stageInstanceName", "myProcessorStage").request().get();
    Assert.assertNotNull(r);

  }

  @Test
  public void testResetOffset() throws IOException {
    Response r = target("/v1/pipeline/resetOffset/myPipeline").request().post(null);
    Assert.assertNotNull(r);

  }

  /*********************************************/
  /*********************************************/

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new PipelineManagerResourceConfig());
        register(PipelineManagerResource.class);
      }
    };
  }

  static class PipelineManagerResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(TestUtil.PipelineManagerTestInjector.class).to(PipelineManager.class);
    }
  }
}
