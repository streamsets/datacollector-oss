/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.PipelineState;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
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

    PipelineState ps = pipelineManager.stopPipeline(false);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(ps).build();
  }

  @Path("/resetOffset/{name}")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response resetOffset(
      @PathParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException {
    pipelineManager.resetOffset(name, rev);
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
      @PathParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getSnapshot(name, rev)).build();
  }

  @Path("/snapshot/{name}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteSnapshot(
      @PathParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev) {
    pipelineManager.deleteSnapshot(name, rev);
    return Response.ok().build();
  }

  @Path("/metrics")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMetrics() {
    Response response;
    if (pipelineManager.getPipelineState() != null && pipelineManager.getPipelineState().getState() == State.RUNNING) {
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
      @PathParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("fromBeginning") @DefaultValue("false") boolean fromBeginning) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getHistory(name, rev, fromBeginning)).build();
  }

  @Path("/history/{pipelineName}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteHistory(
    @PathParam("pipelineName") String pipelineName,
    @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException {
    pipelineManager.deleteHistory(pipelineName, rev);
    return Response.ok().build();
  }

  @Path("/errors/{pipelineName}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getErrors(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") @DefaultValue("0") String revs) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getErrors(pipelineName, revs)).build();
  }

  @Path("/errors/{pipelineName}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteErrorRecords(
      @PathParam("pipelineName") String pipelineName,
      @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException {
    pipelineManager.deleteErrors(pipelineName, rev);
    return Response.ok().build();
  }

  @Path("/errorRecords")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getErrorRecords(
      @QueryParam ("stageInstanceName") @DefaultValue("") String stageInstanceName) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        pipelineManager.getErrorRecords(stageInstanceName)).build();
  }

  @Path("/errorMessages")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getErrorMessages(
      @QueryParam ("stageInstanceName") @DefaultValue("") String stageInstanceName
  ) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        pipelineManager.getErrorMessages(stageInstanceName)).build();
  }

  @Path("/sampledRecords")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSampledRecords(
    @QueryParam ("sampleId") String sampleId,
    @QueryParam ("sampleSize") @DefaultValue("10") int sampleSize) throws PipelineManagerException {
    sampleSize = sampleSize > 100 ? 100 : sampleSize;
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.getSampledRecords(sampleId, sampleSize)).build();
  }

  @Path("/alerts/{pipelineName}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteAlert(
    @PathParam("pipelineName") String pipelineName,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam("alertId") String alertId) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.deleteAlert(alertId)).build();
  }
}
