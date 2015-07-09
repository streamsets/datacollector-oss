/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.PipelineState;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.ErrorMessageJson;
import com.streamsets.pipeline.restapi.bean.MetricRegistryJson;
import com.streamsets.pipeline.restapi.bean.PipelineStateJson;
import com.streamsets.pipeline.restapi.bean.RecordJson;
import com.streamsets.pipeline.restapi.bean.SnapshotInfoJson;
import com.streamsets.pipeline.restapi.bean.SnapshotStatusJson;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.AuthzRole;
import com.streamsets.pipeline.util.ContainerError;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;

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
import java.io.InputStream;
import java.util.Collections;

@Path("/v1/pipeline")
@Api(value = "pipeline")
@DenyAll
public class PipelineManagerResource {

  private final PipelineManager pipelineManager;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public PipelineManagerResource(PipelineManager pipelineManager, RuntimeInfo runtimeInfo) {
    this.pipelineManager = pipelineManager;
    this.runtimeInfo = runtimeInfo;
  }

  @Path("/status")
  @GET
  @ApiOperation(value = "Returns Pipeline Status", response = PipelineStateJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getStatus() throws PipelineManagerException {
    PipelineState ps  = pipelineManager.getPipelineState();
    if (ps != null) {
      ps.getAttributes().put("sdc.updateInfo", pipelineManager.getUpdateInfo());
    }
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapPipelineState(ps)).build();
  }

  @Path("/start")
  @POST
  @ApiOperation(value = "Start Pipeline", response = PipelineStateJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response startPipeline(
      @QueryParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev)
      throws PipelineStoreException, PipelineRuntimeException, StageException, PipelineManagerException {
    Utils.checkState(runtimeInfo.getExecutionMode() != RuntimeInfo.ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");
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
  @ApiOperation(value = "Stop Pipeline", response = PipelineStateJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response stopPipeline() throws PipelineManagerException {
    Utils.checkState(runtimeInfo.getExecutionMode() != RuntimeInfo.ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");
    PipelineState ps = pipelineManager.stopPipeline(false);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapPipelineState(ps)).build();
  }

  @Path("/resetOffset/{pipelineName}")
  @POST
  @ApiOperation(value = "Reset Origin Offset", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response resetOffset(
      @PathParam("pipelineName") String name,
      @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException {
    Utils.checkState(runtimeInfo.getExecutionMode() != RuntimeInfo.ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");
    pipelineManager.resetOffset(name, rev);
    return Response.ok().build();
  }

  @Path("/snapshots/{snapshotName}")
  @PUT
  @ApiOperation(value = "Capture Snapshot", authorizations = @Authorization(value = "basic"))
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response captureSnapshot(
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("batchSize") int batchSize) throws PipelineManagerException {
    pipelineManager.captureSnapshot(snapshotName, batchSize);
    return Response.ok().build();
  }


  @Path("/snapshots")
  @GET
  @ApiOperation(value = "Returns all Snapshot Info", response = SnapshotInfoJson.class, responseContainer = "List",
    authorizations = @Authorization(value = "basic"))
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response getSnapshotsInfo() throws PipelineManagerException, PipelineStoreException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapSnapshotInfo(
      pipelineManager.getSnapshotsInfo())).build();
  }

  @Path("/snapshots/{snapshotName}")
  @GET
  @ApiOperation(value = "Return Snapshot status", response = SnapshotStatusJson.class,
    authorizations = @Authorization(value = "basic"))
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
  @ApiOperation(value = "Return Snapshot data", response = Object.class,
    authorizations = @Authorization(value = "basic"))
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
  @ApiOperation(value = "Delete Snapshot data", authorizations = @Authorization(value = "basic"))
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
  @ApiOperation(value = "Return Pipeline Metrics", response = MetricRegistryJson.class,
    authorizations = @Authorization(value = "basic"))
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

  @Path("/history/{pipelineName}")
  @GET
  @ApiOperation(value = "Find history by pipeline name", response = PipelineStateJson.class, responseContainer = "List",
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getHistory(
      @PathParam("pipelineName") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("fromBeginning") @DefaultValue("false") boolean fromBeginning) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      BeanHelper.wrapPipelineStates(pipelineManager.getHistory(name, rev, fromBeginning))).build();
  }

  @Path("/history/{pipelineName}")
  @DELETE
  @ApiOperation(value = "Delete history by pipeline name", authorizations = @Authorization(value = "basic"))
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
  @ApiOperation(value = "Returns error records by stage instance name and size", response = RecordJson.class,
    responseContainer = "List", authorizations = @Authorization(value = "basic"))
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
  @ApiOperation(value = "Returns error messages by stage instance name and size", response = ErrorMessageJson.class,
   responseContainer = "List", authorizations = @Authorization(value = "basic"))
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
  @ApiOperation(value = "Returns Sampled records by sample ID and size", response = RecordJson.class,
    responseContainer = "List", authorizations = @Authorization(value = "basic"))
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
  @ApiOperation(value = "Delete alert by Pipeline name, revision and Alert ID", response = Boolean.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response deleteAlert(
    @PathParam("pipelineName") String pipelineName,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam("alertId") String alertId) throws PipelineManagerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      pipelineManager.deleteAlert(pipelineName, rev, alertId)).build();
  }

}
