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
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.PipelineState;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.production.SourceOffset;
import com.streamsets.pipeline.store.PipelineStoreException;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;

@Path("/v1/pipeline")
public class PipelineManagerResource {

  private final ProductionPipelineManagerTask pipelineManager;

  @Inject
  public PipelineManagerResource(ProductionPipelineManagerTask pipelineManager) {
    this.pipelineManager = pipelineManager;

  }

  @Path("/status")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatus() throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getPipelineState()).build();
  }

  @Path("/start")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response startPipeline(
      @QueryParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev)
      throws PipelineStoreException, PipelineRuntimeException, StageException, PipelineManagerException {

    PipelineState ps = pipelineManager.startPipeline(name, rev);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(ps).build();
  }

  @Path("/stop")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response stopPipeline() throws PipelineManagerException {

    PipelineState ps = pipelineManager.stopPipeline();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(ps).build();
  }

  @Path("/offset")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response setOffset(
      @QueryParam("offset") String offset) throws PipelineManagerException {
    SourceOffset so = new SourceOffset(pipelineManager.setOffset(offset));
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(so).build();
  }

  @Path("/resetOffset/{name}")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response resetOffset(
      @PathParam("name") String name) throws PipelineManagerException {
    pipelineManager.resetOffset(name);
    return Response.ok().build();
  }

  @Path("/snapshot")
  @PUT
  public Response captureSnapshot(
      @QueryParam("batchSize") int batchSize) throws PipelineManagerException {
    pipelineManager.captureSnapshot(batchSize);
    return Response.ok().build();
  }

  @Path("/snapshot")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSnapshotStatus() {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getSnapshotStatus()).build();
  }

  @Path("/snapshot/{name}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSnapshot(
      @PathParam("name") String name) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getSnapshot(name)).build();
  }

  @Path("/snapshot/{name}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteSnapshot(
      @PathParam("name") String name) {
    pipelineManager.deleteSnapshot(name);
    return Response.ok().build();
  }

  @Path("/metrics")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMetrics() {
    Response response;
    if (pipelineManager.getPipelineState().getState() == State.RUNNING) {
      response = Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getMetrics()).build();
    } else {
      response = Response.ok().type(MediaType.APPLICATION_JSON).entity(Collections.EMPTY_MAP).build();
    }
    return response;
  }

  @Path("/history/{name}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getHistory(
      @PathParam("name") String name) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getHistory(name)).build();
  }

  @Path("/errorRecords/{pipelineName}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getErrorRecords(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam ("stageInstanceName") String stageInstanceName) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getErrorRecords(pipelineName, rev,
        stageInstanceName)).build();
  }

  @Path("/errorRecords")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getErrorRecords(
      @QueryParam ("stageInstanceName") String stageInstanceName) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        pipelineManager.getErrorRecords(stageInstanceName)).build();
  }

  @Path("/errorRecords/{pipelineName}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteErrorRecords(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam ("stageInstanceName") String stageInstanceName) throws PipelineStoreException {
    pipelineManager.deleteErrorRecords(pipelineName, rev, stageInstanceName);
    return Response.ok().build();
  }

}
