/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.PipelineState;
import com.streamsets.pipeline.prodmanager.PipelineStateException;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import java.io.*;

import static org.mockito.Matchers.anyString;

public class TestPipelineManagerResource extends JerseyTest {

  @Test
  public void testGetStatusAPI() {
    PipelineState state = target("/v1/pipeline/status").request().get(PipelineState.class);
    Assert.assertNotNull(state);
    Assert.assertEquals(State.NOT_RUNNING, state.getState());
    Assert.assertEquals("Pipeline is not running", state.getMessage());
  }

  @Test
  public void testStartPipelineAPI() {
    Response r = target("/v1/pipeline/start").queryParam("name", "xyz").queryParam("rev", "2.0")
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
    Assert.assertEquals(State.NOT_RUNNING, state.getState());
    Assert.assertEquals("The pipeline is not running", state.getMessage());
  }

  @Test
  public void testSetOffsetAPI() {
    Entity<String> stringEntity = Entity.entity("", MediaType.APPLICATION_JSON);
    Response r = target("/v1/pipeline/offset").queryParam("name", "xyz").queryParam("rev", "1.0")
        .queryParam("offset", "myOffset").request().put(stringEntity);

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
    Response r = target("/v1/pipeline/snapshot").request().get();
    Assert.assertNotNull(r);

    StringWriter writer = new StringWriter();
    IOUtils.copy((InputStream)r.getEntity(), writer);
    Assert.assertEquals("{\"offset\" : \"abc\"}", writer.toString());

  }

  @Test
  public void testDeleteSnapshotAPI() throws IOException {
    Response r = target("/v1/pipeline/snapshot").request().delete();
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
        Mockito.when(pipelineManager.startPipeline("xyz", "2.0")).thenReturn(new PipelineState(
            "xyz", "2.0", State.RUNNING, "The pipeline is now running", System.currentTimeMillis()));
      } catch (PipelineStateException e) {
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
            new PipelineState("xyz", "2.0", State.NOT_RUNNING, "The pipeline is not running", System.currentTimeMillis()));
      } catch (PipelineStateException e) {
        e.printStackTrace();
      }

      Mockito.when(pipelineManager.getPipelineState()).thenReturn(new PipelineState("xyz", "2.0", State.NOT_RUNNING
          , "Pipeline is not running", System.currentTimeMillis()));
      try {
        Mockito.when(pipelineManager.setOffset(anyString())).thenReturn("fileX:line10");
      } catch (PipelineStateException e) {
        e.printStackTrace();
      }

      Mockito.when(pipelineManager.getSnapshot())
          .thenReturn(getClass().getClassLoader().getResourceAsStream("snapshot.json"))
          .thenReturn(null);

      Mockito.when(pipelineManager.getSnapshotStatus()).thenReturn(new SnapshotStatus(false, true));

      return pipelineManager;
    }

    @Override
    public void dispose(ProductionPipelineManagerTask pipelineStore) {
    }
  }
}
