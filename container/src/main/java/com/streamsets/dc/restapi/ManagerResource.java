/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi;

import com.streamsets.dc.execution.Manager;
import com.streamsets.dc.execution.PipelineStatus;
import com.streamsets.dc.execution.Runner;
import com.streamsets.dc.execution.runner.common.PipelineRunnerException;
import com.streamsets.dc.restapi.bean.PipelineStateJson;
import com.streamsets.dc.restapi.bean.SnapshotInfoJson;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.restapi.bean.ErrorMessageJson;
import com.streamsets.pipeline.restapi.bean.MetricRegistryJson;
import com.streamsets.pipeline.restapi.bean.RecordJson;
import com.streamsets.pipeline.restapi.bean.SnapshotStatusJson;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.AuthzRole;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineException;
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
import java.security.Principal;

@Path("/v2/pipeline")
@Api(value = "pipeline")
@DenyAll
public class ManagerResource {

  private final String user;
  private final Manager manager;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public ManagerResource(Manager manager, RuntimeInfo runtimeInfo, Principal user) {
    this.manager = manager;
    this.runtimeInfo = runtimeInfo;
    this.user = user.getName();
  }

  @Path("/status")
  @GET
  @ApiOperation(value = "Returns Pipeline Status", response = PipelineStateJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getStatus(
    @QueryParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException, PipelineStoreException {
    Runner runner = manager.getRunner(user, name, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(new PipelineStateJson(runner.getStatus())).build();
    }
    return Response.noContent().build();
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
    throws PipelineStoreException, PipelineRuntimeException, StageException, PipelineManagerException, PipelineRunnerException {
    Utils.checkState(runtimeInfo.getExecutionMode() != RuntimeInfo.ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");
    try {
      Runner runner = manager.getRunner(user, name, rev);
      runner.start();
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(new PipelineStateJson(runner.getStatus())).build();
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
  public Response stopPipeline(
    @QueryParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException, PipelineStoreException,
    PipelineRunnerException {
    Utils.checkState(runtimeInfo.getExecutionMode() != RuntimeInfo.ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");
    Runner runner = manager.getRunner(user, name, rev);
    runner.stop();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(new PipelineStateJson(runner.getStatus())).build();
  }

  @Path("/resetOffset/{pipelineName}")
  @POST
  @ApiOperation(value = "Reset Origin Offset", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response resetOffset(
      @PathParam("pipelineName") String name,
      @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineManagerException, PipelineStoreException,
    PipelineRunnerException {
    Utils.checkState(runtimeInfo.getExecutionMode() != RuntimeInfo.ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");
    Runner runner = manager.getRunner(user, name, rev);
    runner.resetOffset();
    return Response.ok().build();
  }

  @Path("/metrics")
  @GET
  @ApiOperation(value = "Return Pipeline Metrics", response = MetricRegistryJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getMetrics(@QueryParam("name") String name,
                             @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineStoreException {
    Runner runner = manager.getRunner(user, name, rev);
    if (runner != null && runner.getStatus().getStatus() == PipelineStatus.RUNNING) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(runner.getMetrics()).build();
    }
    return Response.noContent().build();
  }

  @Path("/snapshots/{snapshotName}")
  @PUT
  @ApiOperation(value = "Capture Snapshot", authorizations = @Authorization(value = "basic"))
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response captureSnapshot(
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("name") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("batchSize") int batchSize) throws PipelineException {
    Runner runner = manager.getRunner(user, name, rev);
    if (runner != null && runner.getStatus().getStatus() == PipelineStatus.RUNNING) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        runner.captureSnapshot(snapshotName, batchSize)).build();
    }
    //FIXME<Hari>: What do you return here?
    return Response.ok().build();
  }

  @Path("/snapshots")
  @GET
  @ApiOperation(value = "Returns all Snapshot Info", response = SnapshotInfoJson.class, responseContainer = "List",
    authorizations = @Authorization(value = "basic"))
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response getSnapshotsInfo(@QueryParam("name") String name,
                                   @QueryParam("rev") @DefaultValue("0") String rev)
    throws PipelineException {
    Runner runner = manager.getRunner(user, name, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapSnapshotInfoNewAPI(
        runner.getSnapshotsInfo())).build();
    }
    return Response.noContent().build();
  }

  @Path("/snapshots/{snapshotName}")
  @GET
  @ApiOperation(value = "Return Snapshot status", response = SnapshotStatusJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response getSnapshotStatus(
    @PathParam("snapshotName") String snapshotName,
    @QueryParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineException {
    Runner runner = manager.getRunner(user, name, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        new SnapshotInfoJson(runner.getSnapshot(snapshotName).getInfo())).build();
    }
    return Response.noContent().build();
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
      @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineException {
    Runner runner = manager.getRunner(user, pipelineName, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(runner.getSnapshot(snapshotName).getOutput()).build();
    }
    return Response.noContent().build();
  }

  @Path("/snapshots/{pipelineName}/{snapshotName}")
  @DELETE
  @ApiOperation(value = "Delete Snapshot data", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response deleteSnapshot(
      @PathParam("pipelineName") String pipelineName,
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineException {
    Runner runner = manager.getRunner(user, pipelineName, rev);
    if(runner != null) {
      runner.deleteSnapshot(snapshotName);
    }
    return Response.ok().build();
  }

  @Path("/history/{pipelineName}")
  @DELETE
  @ApiOperation(value = "Delete history by pipeline name", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response deleteHistory(
    @PathParam("pipelineName") String pipelineName,
    @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineException {
    Runner runner = manager.getRunner(user, pipelineName, rev);
    if(runner != null) {
      runner.deleteHistory();
    }
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
      @QueryParam("pipelineName") String pipelineName,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam ("stageInstanceName") @DefaultValue("") String stageInstanceName,
      @QueryParam ("size") @DefaultValue("10") int size) throws PipelineException {
    size = size > 100 ? 100 : size;
    Runner runner = manager.getRunner(user, pipelineName, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapRecords(runner.getErrorRecords(stageInstanceName, size))).build();
    }
    return Response.noContent().build();
  }

  @Path("/errorMessages")
  @GET
  @ApiOperation(value = "Returns error messages by stage instance name and size", response = ErrorMessageJson.class,
   responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response getErrorMessages(
      @QueryParam("pipelineName") String pipelineName,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam ("stageInstanceName") @DefaultValue("") String stageInstanceName,
      @QueryParam ("size") @DefaultValue("10") int size
  ) throws PipelineException {
    size = size > 100 ? 100 : size;
    Runner runner = manager.getRunner(user, pipelineName, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapErrorMessages(runner.getErrorMessages(stageInstanceName, size))).build();
    }
    return Response.noContent().build();
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
    @QueryParam("fromBeginning") @DefaultValue("false") boolean fromBeginning) throws PipelineManagerException, PipelineStoreException {
    Runner runner = manager.getRunner(user, name, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapPipelineStatesNewAPI(runner.getHistory())).build();
    }
    return Response.noContent().build();
  }

  @Path("/sampledRecords")
  @GET
  @ApiOperation(value = "Returns Sampled records by sample ID and size", response = RecordJson.class,
    responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response getSampledRecords(
    @QueryParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam ("sampleId") String sampleId,
    @QueryParam ("sampleSize") @DefaultValue("10") int sampleSize) throws PipelineManagerException,
    PipelineStoreException, PipelineRunnerException {
    sampleSize = sampleSize > 100 ? 100 : sampleSize;
    Runner runner = manager.getRunner(user, name, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapRecords(runner.getSampledRecords(sampleId, sampleSize))).build();
    }
    return Response.noContent().build();
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
    @QueryParam("alertId") String alertId) throws PipelineManagerException, PipelineStoreException,
    PipelineRunnerException {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      manager.getRunner(user, pipelineName, rev).deleteAlert(alertId)).build();
  }

}
