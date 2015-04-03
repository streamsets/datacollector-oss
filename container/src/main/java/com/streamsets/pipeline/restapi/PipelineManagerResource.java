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
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.ContainerError;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
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
@DenyAll
public class PipelineManagerResource {

  private final ProductionPipelineManagerTask pipelineManager;

  @Inject
  public PipelineManagerResource(ProductionPipelineManagerTask pipelineManager) {
    this.pipelineManager = pipelineManager;

  }

  @Path("/status")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getStatus() throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      BeanHelper.wrapPipelineState(pipelineManager.getPipelineState())).build();
  }

  @Path("/start")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response startPipeline(
      @QueryParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev)
      throws PipelineStoreException, PipelineRuntimeException, StageException, PipelineManagerException {
    try {
      PipelineState ps = pipelineManager.startPipeline(name, rev);
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapPipelineState(ps)).build();
    } catch (PipelineRuntimeException ex) {
      if (ex.getErrorCode() == ContainerError.CONTAINER_0165) {
        return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(
          BeanHelper.wrapIssues(ex.getIssues())).build();
      } else {
        throw ex;
      }
    }
  }

  @Path("/stop")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response stopPipeline() throws PipelineManagerException {
    PipelineState ps = pipelineManager.stopPipeline(false);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapPipelineState(ps)).build();
  }

  @Path("/resetOffset/{name}")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response resetOffset(
      @PathParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException {
    pipelineManager.resetOffset(name, rev);
    return Response.ok().build();
  }

  @Path("/snapshots/{snapshotName}")
  @PUT
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response captureSnapshot(
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("batchSize") int batchSize) throws PipelineManagerException {
    pipelineManager.captureSnapshot(snapshotName, batchSize);
    return Response.ok().build();
  }


  @Path("/snapshots")
  @GET
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response getSnapshotsInfo() throws PipelineManagerException, PipelineStoreException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapSnapshotInfo(
      pipelineManager.getSnapshotsInfo())).build();
  }

  @Path("/snapshots/{snapshotName}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response getSnapshotStatus(
    @PathParam("snapshotName") String snapshotName,
    @QueryParam("rev") @DefaultValue("0") String rev) {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      BeanHelper.wrapSnapshotStatus(pipelineManager.getSnapshotStatus(snapshotName))).build();
  }

  @Path("/snapshots/{pipelineName}/{snapshotName}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response getSnapshot(
      @PathParam("pipelineName") String pipelineName,
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineManager.getSnapshot(pipelineName, rev,
      snapshotName)).build();
  }

  @Path("/snapshots/{pipelineName}/{snapshotName}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response deleteSnapshot(
      @PathParam("pipelineName") String pipelineName,
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("rev") @DefaultValue("0") String rev) {
    pipelineManager.deleteSnapshot(pipelineName, rev, snapshotName);
    return Response.ok().build();
  }

  @Path("/metrics")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
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
  @PermitAll
  public Response getHistory(
      @PathParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("fromBeginning") @DefaultValue("false") boolean fromBeginning) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      BeanHelper.wrapPipelineStates(pipelineManager.getHistory(name, rev, fromBeginning))).build();
  }

  @Path("/history/{pipelineName}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response deleteHistory(
    @PathParam("pipelineName") String pipelineName,
    @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException {
    pipelineManager.deleteHistory(pipelineName, rev);
    return Response.ok().build();
  }

  @Path("/errorRecords")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response getErrorRecords(
      @QueryParam ("stageInstanceName") @DefaultValue("") String stageInstanceName,
      @QueryParam ("size") @DefaultValue("10") int size) throws PipelineManagerException {
    size = size > 100 ? 100 : size;
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      BeanHelper.wrapRecords(pipelineManager.getErrorRecords(stageInstanceName, size))).build();
  }

  @Path("/errorMessages")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response getErrorMessages(
      @QueryParam ("stageInstanceName") @DefaultValue("") String stageInstanceName,
      @QueryParam ("size") @DefaultValue("10") int size
  ) throws PipelineManagerException {
    size = size > 100 ? 100 : size;
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapErrorMessages(pipelineManager.getErrorMessages(stageInstanceName, size))).build();
  }

  @Path("/sampledRecords")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response getSampledRecords(
    @QueryParam ("sampleId") String sampleId,
    @QueryParam ("sampleSize") @DefaultValue("10") int sampleSize) throws PipelineManagerException {
    sampleSize = sampleSize > 100 ? 100 : sampleSize;
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      BeanHelper.wrapRecords(pipelineManager.getSampledRecords(sampleId, sampleSize))).build();
  }

  @Path("/alerts/{pipelineName}")
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response deleteAlert(
    @PathParam("pipelineName") String pipelineName,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam("alertId") String alertId) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.deleteAlert(alertId)).build();
  }

}
