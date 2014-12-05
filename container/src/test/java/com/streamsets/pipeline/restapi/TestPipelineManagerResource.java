/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.codahale.metrics.MetricRegistry;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.PipelineState;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.SourceOffset;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.store.PipelineStoreException;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Matchers;

public class TestPipelineManagerResource extends JerseyTest {
  
  private static final String PIPELINE_NAME = "myPipeline";

  @Test
  public void testGetStatusAPI() {
    PipelineState state = target("/v1/pipeline/status").request().get(PipelineState.class);
    Assert.assertNotNull(state);
    Assert.assertEquals(State.STOPPED, state.getState());
    Assert.assertEquals("Pipeline is not running", state.getMessage());
  }

  @Test
  public void testStartPipelineAPI() {
    Response r = target("/v1/pipeline/start").queryParam("name", PIPELINE_NAME).queryParam("rev", "2.0")
        .request().post(null);
    Assert.assertNotNull(r);
    PipelineState state = r.readEntity(PipelineState.class);
    Assert.assertNotNull(state);
    Assert.assertEquals(State.RUNNING, state.getState());
    Assert.assertEquals("The pipeline is now running", state.getMessage());
  }

  @Test
  public void testStopPipelineAPI() {
    Response r = target("/v1/pipeline/stop").queryParam("rev", "2.0")
        .request().post(null);
    Assert.assertNotNull(r);
    PipelineState state = r.readEntity(PipelineState.class);
    Assert.assertNotNull(state);
    Assert.assertEquals(State.STOPPED, state.getState());
    Assert.assertEquals("The pipeline is not running", state.getMessage());
  }

  @Test
  public void testSetOffsetAPI() {
    Response r = target("/v1/pipeline/offset").queryParam("offset", "myOffset").request().post(null);

    Assert.assertNotNull(r);
    SourceOffset so = r.readEntity(SourceOffset.class);
    Assert.assertNotNull(so);
    Assert.assertEquals("fileX:line10", so.getOffset());

  }

  @Test
   public void testCaptureSnapshotAPI() {
    Entity<String> stringEntity = Entity.entity("", MediaType.APPLICATION_JSON);
    Response r = target("/v1/pipeline/snapshot").request().put(stringEntity);
    Assert.assertNotNull(r);
  }

  @Test
  public void testGetSnapshotAPI() throws IOException {
    Response r = target("/v1/pipeline/snapshot/myPipeline").request().get();
    Assert.assertNotNull(r);

    StringWriter writer = new StringWriter();
    IOUtils.copy((InputStream)r.getEntity(), writer);
    Assert.assertEquals("{\"offset\" : \"abc\"}", writer.toString());

  }

  @Test
  public void testDeleteSnapshotAPI() throws IOException {
    Response r = target("/v1/pipeline/snapshot/myPipeline").request().delete();
    Assert.assertNotNull(r);

  }

  @Test
  public void testSnapshotStatusAPI() throws IOException {
    Response r = target("/v1/pipeline/snapshot").queryParam("get","status").request().get();
    Assert.assertNotNull(r);

    SnapshotStatus s = r.readEntity(SnapshotStatus.class);

    Assert.assertEquals(false, s.isExists());
    Assert.assertEquals(true, s.isSnapshotInProgress());
  }

  @Test
  public void testGetMerticsAPI() {
    Response r = target("/v1/pipeline/metrics").queryParam("get","status").request().get();
    Assert.assertNotNull(r);

    MetricRegistry m = r.readEntity(MetricRegistry.class);
    Assert.assertNotNull(m);
  }

  //List<PipelineState> pipelineStates
  @Test
  public void testGetHistoryAPI() {
    Response r = target("/v1/pipeline/history/myPipeline").request().get();
    Assert.assertNotNull(r);
    List<PipelineState> pipelineStates = r.readEntity(new GenericType<List<PipelineState>>() {});
    Assert.assertEquals(3, pipelineStates.size());
  }

  @Test
  public void testDeleteErrorRecordsAPI() throws IOException {
    Response r = target("/v1/pipeline/errorRecords/myPipeline").request().delete();
    Assert.assertNotNull(r);

  }

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
      bindFactory(PipelineManagerTestInjector.class).to(ProductionPipelineManagerTask.class);
    }
  }

  static class PipelineManagerTestInjector implements Factory<ProductionPipelineManagerTask> {

    public PipelineManagerTestInjector() {
    }

    @Singleton
    @Override
    public ProductionPipelineManagerTask provide() {


      ProductionPipelineManagerTask pipelineManager = Mockito.mock(ProductionPipelineManagerTask.class);
      try {
        Mockito.when(pipelineManager.startPipeline(PIPELINE_NAME, "2.0")).thenReturn(new PipelineState(
            PIPELINE_NAME, "2.0", State.RUNNING, "The pipeline is now running", System.currentTimeMillis()));
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      } catch (StageException e) {
        e.printStackTrace();
      } catch (PipelineRuntimeException e) {
        e.printStackTrace();
      } catch (PipelineStoreException e) {
        e.printStackTrace();
      }

      try {
        Mockito.when(pipelineManager.stopPipeline()).thenReturn(
            new PipelineState(PIPELINE_NAME, "2.0", State.STOPPED, "The pipeline is not running", System.currentTimeMillis()));
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      Mockito.when(pipelineManager.getPipelineState()).thenReturn(new PipelineState(PIPELINE_NAME, "2.0", State.STOPPED
          , "Pipeline is not running", System.currentTimeMillis()));
      try {
        Mockito.when(pipelineManager.setOffset(Matchers.anyString())).thenReturn("fileX:line10");
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      try {
        Mockito.when(pipelineManager.getSnapshot(PIPELINE_NAME))
            .thenReturn(getClass().getClassLoader().getResourceAsStream("snapshot.json"))
            .thenReturn(null);
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      Mockito.when(pipelineManager.getSnapshotStatus()).thenReturn(new SnapshotStatus(false, true));

      Mockito.when(pipelineManager.getMetrics()).thenReturn(new MetricRegistry());

      List<PipelineState> states = new ArrayList<>();
      states.add(new PipelineState(PIPELINE_NAME, "1", State.STOPPED, "", System.currentTimeMillis()));
      states.add(new PipelineState(PIPELINE_NAME, "1", State.RUNNING, "", System.currentTimeMillis()));
      states.add(new PipelineState(PIPELINE_NAME, "1", State.STOPPED, "", System.currentTimeMillis()));
      try {
        Mockito.when(pipelineManager.getHistory(PIPELINE_NAME)).thenReturn(states);
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      Mockito.doNothing().when(pipelineManager).deleteSnapshot(PIPELINE_NAME);
      Mockito.doNothing().when(pipelineManager).deleteErrors(PIPELINE_NAME, "1");

      return pipelineManager;
    }

    @Override
    public void dispose(ProductionPipelineManagerTask pipelineManagerTask) {
    }
  }
}
