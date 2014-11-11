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

import com.streamsets.pipeline.agent.RuntimeInfo;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DeliveryGuarantee;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.container.Configuration;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.ProductionPipeline;
import com.streamsets.pipeline.runner.production.ProductionPipelineBuilder;
import com.streamsets.pipeline.runner.production.ProductionPipelineRunner;
import com.streamsets.pipeline.runner.production.ProductionSourceOffsetTracker;
import com.streamsets.pipeline.stagelibrary.StageLibrary;
import com.streamsets.pipeline.state.PipelineStateErrors;
import com.streamsets.pipeline.state.PipelineStateException;
import com.streamsets.pipeline.state.PipelineStateManager;
import com.streamsets.pipeline.state.State;
import com.streamsets.pipeline.store.PipelineStore;
import com.streamsets.pipeline.store.PipelineStoreException;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/pipelines")
public class PipelineStateResource {

  private static final String MAX_BATCH_SIZE_KEY = "maxBatchSize";
  private static final int MAX_BATCH_SIZE_DEFAULT = 10;

  private final PipelineStateManager pipelineStateManager;
  private final Configuration configuration;
  private final PipelineStore pipelineStore;
  private final StageLibrary stageLibrary;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public PipelineStateResource(PipelineStateManager pipelineStateManager,
                               Configuration configuration,
                               PipelineStore pipelineStore,
                               StageLibrary stageLib,
                               RuntimeInfo runtimeInfo) {
    this.pipelineStateManager = pipelineStateManager;
    this.configuration = configuration;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLib;
    this.runtimeInfo = runtimeInfo;
  }

  @Path("/status")
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response getStatus() throws PipelineStateException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineStateManager.getState()).build();
  }

  @Path("/status")
  @POST
  @Produces(MediaType.TEXT_PLAIN)
  public Response setStatus(
    @QueryParam("name") String name,
    @QueryParam("rev") String rev,
    @QueryParam("state") String state) throws PipelineStoreException, PipelineRuntimeException, StageException, PipelineStateException {

    State currentState
            = pipelineStateManager.getState();

    if(!currentState.isValidTransition(state)) {
      throw new PipelineStateException(
          PipelineStateErrors.INVALID_STATE_TRANSITION, currentState, state);
    }

    if(currentState == State.RUNNING) {
      if(state.equals("NOT_RUNNING")) {
        //TODO
        pipelineStateManager.setState(State.NOT_RUNNING);
        pipelineStateManager.stopPipeline();
        return Response.ok().type(MediaType.TEXT_PLAIN).entity(
            pipelineStateManager.getState().name()).build();
      }
      //TODO handle error, will UI ever make this call?
      if(state.equals("ERROR")) {

      }
    } else if (currentState == State.NOT_RUNNING) {
      if (state.equals("RUNNING")) {
        pipelineStateManager.setState(State.RUNNING);
        pipelineStateManager.runPipeline(createProductionPipeline(name, rev));
        return Response.ok().type(MediaType.TEXT_PLAIN).entity(
            State.RUNNING.name()).build();
      }
    } else if(currentState == State.ERROR) {
      if (state.equals("NOT_RUNNING")) {
        //this means that the client has acknowledged the error
        pipelineStateManager.setState(State.NOT_RUNNING);
        return Response.ok().type(MediaType.TEXT_PLAIN).entity(
            State.NOT_RUNNING.name()).build();
      }
    }

    //bug
    return null;
  }

  private ProductionPipeline createProductionPipeline(String name, String rev) throws PipelineStoreException,
      PipelineRuntimeException, StageException {

    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.valueOf(
      configuration.get("deliveryGuarantee", "AT_LEAST_ONCE"));

    PipelineConfiguration pipelineConfiguration = pipelineStore.load(name, rev);

    ProductionSourceOffsetTracker productionSourceOffsetTracker = new ProductionSourceOffsetTracker(runtimeInfo);
    String sourceOffset = productionSourceOffsetTracker.getOffset();

    ProductionPipelineRunner runner = new ProductionPipelineRunner(
      productionSourceOffsetTracker, maxBatchSize, deliveryGuarantee);

    ProductionPipelineBuilder builder = new ProductionPipelineBuilder(
      stageLibrary, name, pipelineConfiguration);

    return builder.build(runner);
  }

}
