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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.AclManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.StartPipelineContextBuilder;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.AlertInfoJson;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.ErrorMessageJson;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import com.streamsets.datacollector.restapi.bean.MultiStatusResponseJson;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.datacollector.restapi.bean.RecordJson;
import com.streamsets.datacollector.restapi.bean.SampledRecordJson;
import com.streamsets.datacollector.restapi.bean.SnapshotDataJson;
import com.streamsets.datacollector.restapi.bean.SnapshotInfoJson;
import com.streamsets.datacollector.restapi.bean.SourceOffsetJson;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.AclPipelineStoreTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.EdgeUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v1")
@Api(value = "manager")
@DenyAll
@RequiresCredentialsDeployed
public class ManagerResource {
  private final String user;
  private final Manager manager;
  private final PipelineStoreTask store;
  private static final Logger LOG = LoggerFactory.getLogger(ManagerResource.class);

  @Inject
  public ManagerResource(
      Manager manager,
      Principal principal,
      PipelineStoreTask store,
      AclStoreTask aclStore,
      RuntimeInfo runtimeInfo,
      UserGroupManager userGroupManager
  ) {
    this.user = principal.getName();

    UserJson currentUser;
    if (runtimeInfo.isDPMEnabled() && !runtimeInfo.isRemoteSsoDisabled()) {
      currentUser = new UserJson((SSOPrincipal)principal);
    } else {
      currentUser = userGroupManager.getUser(principal);
    }

    if (runtimeInfo.isAclEnabled()) {
      this.manager = new AclManager(manager, aclStore, currentUser);
      this.store = new AclPipelineStoreTask(store, aclStore, currentUser);
    } else {
      this.manager = manager;
      this.store = store;
    }
  }

  @Path("/pipelines/status")
  @GET
  @ApiOperation(value = "Returns all Pipeline Status", response = PipelineStateJson.class,
    responseContainer = "Map", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getAllPipelineStatus() throws PipelineException {
    RestAPIUtils.injectPipelineInMDC("*");
    List<PipelineState> pipelineStateList = manager.getPipelines();
    Map<String, PipelineStateJson> pipelineStateMap = new HashMap<>();
    for(PipelineState pipelineState: pipelineStateList) {
      pipelineStateMap.put(pipelineState.getPipelineId(), BeanHelper.wrapPipelineState(pipelineState, true));
    }
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(pipelineStateMap).build();
  }

  @Path("/pipeline/{pipelineId}/status")
  @GET
  @ApiOperation(value = "Returns Pipeline Status for the given pipeline", response = PipelineStateJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelineStatus(
    @PathParam("pipelineId") String pipelineId,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam("onlyIfExists") boolean onlyIfExists
  ) throws PipelineException {
    try {
      PipelineInfo pipelineInfo = store.getInfo(pipelineId);
      RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
      if(pipelineId != null) {
        return Response.ok()
            .type(MediaType.APPLICATION_JSON)
            .entity(BeanHelper.wrapPipelineState(manager.getPipelineState(pipelineId, rev))).build();
      }
    } catch (PipelineException ex) {
      if (onlyIfExists && ContainerError.CONTAINER_0200.equals(ex.getErrorCode())) {
        // If new query param onlyIfExists passed, return pipeline metrics only pipelineId exists otherwise return
        // null instead of throwing pipeline not found exception
        return Response.ok(null).build();
      }
      throw ex;
    }
    return Response.noContent().build();
  }

  @Path("/pipeline/{pipelineId}/start")
  @POST
  @ApiOperation(value = "Start Pipeline", response = PipelineStateJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response startPipeline(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @ApiParam(
          name = "runtimeParameters",
          value = "Runtime Parameters to override Pipeline Parameters value"
      ) Map<String, Object> runtimeParameters
  ) throws StageException, PipelineException {
    if(pipelineId != null) {
      PipelineInfo pipelineInfo = store.getInfo(pipelineId);
      RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
      if (manager.isRemotePipeline(pipelineId, rev)) {
        throw new PipelineException(ContainerError.CONTAINER_01101, "START_PIPELINE", pipelineId);
      }

      PipelineState pipelineState = manager.getPipelineState(pipelineId, rev);

      if (pipelineState.getExecutionMode() == ExecutionMode.SLAVE) {
        throw new PipelineException(
            ContainerError.CONTAINER_01601,
            pipelineId,
            pipelineState.getExecutionMode()
        );
      }

      try {
        Runner runner = manager.getRunner(pipelineId, rev);
        runner.start(new StartPipelineContextBuilder(user).withRuntimeParameters(runtimeParameters).build());
        return Response.ok()
            .type(MediaType.APPLICATION_JSON)
            .entity(BeanHelper.wrapPipelineState(runner.getState())).build();
      } catch (PipelineRuntimeException ex) {
        if (ex.getErrorCode() == ContainerError.CONTAINER_0165) {
          return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(
              BeanHelper.wrapIssues(ex.getIssues())).build();
        } else {
          throw ex;
        }
      }
    }

    return Response.noContent().build();
  }

  @Path("/pipelines/start")
  @POST
  @ApiOperation(value = "Start multiple Pipelines", response = MultiStatusResponseJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response startPipelines(List<String> pipelineIds) throws StageException, PipelineException {
    List<PipelineState> successEntities = new ArrayList<>();
    List<String> errorMessages = new ArrayList<>();

    for (String pipelineId: pipelineIds) {
      if (pipelineId != null) {
        PipelineInfo pipelineInfo = store.getInfo(pipelineId);
        RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());

        if (manager.isRemotePipeline(pipelineId, "0")) {
          errorMessages.add("Cannot start a remote pipeline: " + pipelineId);
          continue;
        }

        PipelineState pipelineState = manager.getPipelineState(pipelineId, "0");

        if (pipelineState.getExecutionMode() == ExecutionMode.SLAVE) {
          errorMessages.add(Utils.format(
              ContainerError.CONTAINER_01601.getMessage(),
              pipelineId,
              pipelineState.getExecutionMode()
          ));
          continue;
        }

        Runner runner = manager.getRunner( pipelineId, "0");
        try {
          runner.start(new StartPipelineContextBuilder(user).build());
          successEntities.add(runner.getState());
        } catch (Exception ex) {
          errorMessages.add("Failed starting pipeline: " + pipelineId + ". Error: " + ex.getMessage());
        }
      }
    }

    return Response.status(207)
        .type(MediaType.APPLICATION_JSON)
        .entity(new MultiStatusResponseJson<>(successEntities, errorMessages)).build();
  }

  @Path("/pipeline/{pipelineId}/stop")
  @POST
  @ApiOperation(value = "Stop Pipeline", response = PipelineStateJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response stopPipeline(
    @PathParam("pipelineId") String pipelineId,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @Context SecurityContext context
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    if (manager.isRemotePipeline(pipelineId, rev) && !context.isUserInRole(AuthzRole.ADMIN) &&
        !context.isUserInRole(AuthzRole.ADMIN_REMOTE)) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "STOP_PIPELINE", pipelineId);
    }
    Runner runner = manager.getRunner(pipelineId, rev);
    Utils.checkState(runner.getState().getExecutionMode() != ExecutionMode.SLAVE,
      "This operation is not supported in SLAVE mode");
    runner.stop(user);
    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(BeanHelper.wrapPipelineState(runner.getState())).build();
  }

  @Path("/pipelines/stop")
  @POST
  @ApiOperation(value = "Stop multiple Pipelines", response = MultiStatusResponseJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response stopPipelines(
      List<String> pipelineIds,
      @Context SecurityContext context
  ) throws StageException, PipelineException {
    List<PipelineState> successEntities = new ArrayList<>();
    List<String> errorMessages = new ArrayList<>();

    for (String pipelineId: pipelineIds) {
      if (pipelineId != null) {
        PipelineInfo pipelineInfo = store.getInfo(pipelineId);
        RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());

        if (manager.isRemotePipeline(pipelineId, "0") && !context.isUserInRole(AuthzRole.ADMIN) &&
            !context.isUserInRole(AuthzRole.ADMIN_REMOTE)) {
          errorMessages.add("Cannot stop a remote pipeline: " + pipelineId);
          continue;
        }
        Runner runner = manager.getRunner(pipelineId, "0");
        try {
          Utils.checkState(runner.getState().getExecutionMode() != ExecutionMode.SLAVE,
              "This operation is not supported in SLAVE mode");
          runner.stop(user);
          successEntities.add(runner.getState());

        } catch (Exception ex) {
          errorMessages.add("Failed stopping pipeline: " + pipelineId + ". Error: " + ex.getMessage());
        }
      }
    }

    return Response.status(207)
        .type(MediaType.APPLICATION_JSON)
        .entity(new MultiStatusResponseJson<>(successEntities, errorMessages)).build();
  }

  @Path("/pipeline/{pipelineId}/forceStop")
  @POST
  @ApiOperation(value = "Force Stop Pipeline", response = PipelineStateJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response forceStopPipeline(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @Context SecurityContext context) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    if (manager.isRemotePipeline(pipelineId, rev)) {
      if (!context.isUserInRole(AuthzRole.ADMIN) && !context.isUserInRole(AuthzRole.ADMIN_REMOTE)) {
        throw new PipelineException(ContainerError.CONTAINER_01101, "FORCE_QUIT_PIPELINE", pipelineId);
      }
    }
    Runner runner = manager.getRunner(pipelineId, rev);
    Utils.checkState(
        runner.getState().getExecutionMode() == ExecutionMode.STANDALONE ||
            runner.getState().getExecutionMode() == ExecutionMode.STREAMING ||
            runner.getState().getExecutionMode() == ExecutionMode.BATCH,
        Utils.format("This operation is not supported in {} mode", runner.getState().getExecutionMode())
    );
    runner.forceQuit(user);
    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(BeanHelper.wrapPipelineState(runner.getState())).build();
  }

  @Path("/pipelines/forceStop")
  @POST
  @ApiOperation(value = "Force Stop multiple Pipelines", response = MultiStatusResponseJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response forceStopPipelines(List<String> pipelineIds, @Context SecurityContext context) throws PipelineException {
    List<PipelineState> successEntities = new ArrayList<>();
    List<String> errorMessages = new ArrayList<>();

    for (String pipelineId: pipelineIds) {
      if (pipelineId != null) {

        PipelineInfo pipelineInfo = store.getInfo(pipelineId);
        RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
        if (manager.isRemotePipeline(pipelineId, "0")) {
          if (!context.isUserInRole(AuthzRole.ADMIN) && !context.isUserInRole(AuthzRole.ADMIN_REMOTE)) {
            errorMessages.add("Cannot force stop a remote pipeline: " + pipelineId);
            continue;
          }
        }

        Runner runner = manager.getRunner(pipelineId, "0");
        try {
          Utils.checkState(
              runner.getState().getExecutionMode() == ExecutionMode.STANDALONE ||
                  runner.getState().getExecutionMode() == ExecutionMode.STREAMING ||
                  runner.getState().getExecutionMode() == ExecutionMode.BATCH,
              Utils.format("This operation is not supported in {} mode", runner.getState().getExecutionMode())
          );
          runner.forceQuit(user);
          successEntities.add(runner.getState());

        } catch (Exception ex) {
          errorMessages.add("Failed to force stop pipeline: " + pipelineId + ". Error: " + ex.getMessage());
        }
      }
    }

    return Response.status(207)
        .type(MediaType.APPLICATION_JSON)
        .entity(new MultiStatusResponseJson<>(successEntities, errorMessages)).build();
  }

  @Path("/pipeline/{pipelineId}/committedOffsets")
  @GET
  @ApiOperation(value = "Return Committed Offsets. Note: Returned offset format will change between releases.",
      response = SourceOffsetJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getCommittedOffsets(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Runner runner = manager.getRunner(pipelineId, rev);
    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(BeanHelper.wrapSourceOffset(runner.getCommittedOffsets()))
        .build();
  }

  @Path("/pipeline/{pipelineId}/committedOffsets")
  @POST
  @ApiOperation(value = "Update Pipeline Committed Offsets.",
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response updateCommittedOffsets(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev,
      SourceOffsetJson sourceOffset
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Runner runner = manager.getRunner(pipelineId, rev);
    runner.updateCommittedOffsets(BeanHelper.unwrapSourceOffset(sourceOffset));
    return Response.ok().build();
  }

  @Path("/pipeline/{pipelineId}/resetOffset")
  @POST
  @ApiOperation(value = "Reset Origin Offset", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response resetOffset(
      @PathParam("pipelineId") String name,
      @QueryParam("rev") @DefaultValue("0") String rev
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(name);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    if (manager.isRemotePipeline(name, rev)) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "RESET_OFFSET", name);
    }
    Runner runner = manager.getRunner(name, rev);
    runner.resetOffset("user");
    return Response.ok().build();
  }

  @Path("/pipelines/resetOffsets")
  @POST
  @ApiOperation(value = "Reset Origin Offset for multiple pipelines", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response resetOffsets(List<String> pipelineIds) throws PipelineException {
    for (String pipelineId: pipelineIds) {
      PipelineInfo pipelineInfo = store.getInfo(pipelineId);
      RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
      if (manager.isRemotePipeline(pipelineId, "0")) {
        throw new PipelineException(ContainerError.CONTAINER_01101, "RESET_OFFSETS", pipelineId);
      }
      Runner runner = manager.getRunner(pipelineId, "0");
      runner.resetOffset(user);
    }
    return Response.ok().build();
  }

  @Path("/pipeline/{pipelineId}/metrics")
  @GET
  @ApiOperation(value = "Return Pipeline Metrics", response = MetricRegistryJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getMetrics(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("onlyIfExists") boolean onlyIfExists
  ) throws PipelineException {
    try {
      PipelineInfo pipelineInfo = store.getInfo(pipelineId);
      RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
      if (pipelineId != null) {
        Runner runner = manager.getRunner(pipelineId, rev);
        if (runner != null && runner.getState().getStatus().isActive()) {
          return Response.ok().type(MediaType.APPLICATION_JSON).entity(runner.getMetrics()).build();
        }
        if (runner != null) {
          LOG.debug("Status is " + runner.getState().getStatus());
        } else {
          LOG.debug("Runner is null");
        }
      }
    } catch (PipelineException ex) {
      if (onlyIfExists && ContainerError.CONTAINER_0200.equals(ex.getErrorCode())) {
        // If new query param onlyIfExists passed, return pipeline metrics only pipelineId exists otherwise return
        // null instead of throwing pipeline not found exception
        return Response.ok(null).build();
      }
      throw ex;
    }
    return Response.noContent().build();
  }

  @Path("/pipeline/{pipelineId}/snapshot/{snapshotName}")
  @PUT
  @ApiOperation(value = "Capture Snapshot", authorizations = @Authorization(value = "basic"))
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN, AuthzRole.MANAGER_REMOTE, AuthzRole.ADMIN_REMOTE })
  public Response captureSnapshot(
      @PathParam("pipelineId") String pipelineId,
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("snapshotLabel") String snapshotLabel,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("batches") @DefaultValue("1") int batches,
      @QueryParam("batchSize") @DefaultValue("10") int batchSize,
      @QueryParam("startPipeline") @DefaultValue("false") boolean startPipeline,
      Map<String, Object> runtimeParameters
  ) throws PipelineException, StageException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Runner runner = manager.getRunner(pipelineId, rev);
    if (startPipeline && runner != null) {
      runner.startAndCaptureSnapshot(
          new StartPipelineContextBuilder(user).withRuntimeParameters(runtimeParameters).build(),
          snapshotName,
          snapshotLabel,
          batches,
          batchSize
        );
    } else {
      Utils.checkState(runner != null && runner.getState().getStatus() == PipelineStatus.RUNNING,
          "Pipeline doesn't exist or it is not running currently");
      runner.captureSnapshot(user, snapshotName, snapshotLabel, batches, batchSize);
    }
    return Response.ok().build();
  }


  @Path("/pipeline/{pipelineId}/snapshot/{snapshotName}")
  @POST
  @ApiOperation(value = "Update Snapshot Label", authorizations = @Authorization(value = "basic"))
  @RolesAllowed({ AuthzRole.MANAGER, AuthzRole.ADMIN, AuthzRole.MANAGER_REMOTE, AuthzRole.ADMIN_REMOTE })
  public Response updateSnapshotLabel(
      @PathParam("pipelineId") String pipelineId,
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("snapshotLabel") String snapshotLabel,
      @QueryParam("rev") @DefaultValue("0") String rev
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Runner runner = manager.getRunner(pipelineId, rev);
    runner.updateSnapshotLabel(snapshotName, snapshotLabel);
    return Response.ok().build();
  }

  @Path("/pipelines/snapshots")
  @GET
  @ApiOperation(value = "Returns all Snapshot Info", response = SnapshotInfoJson.class, responseContainer = "List",
    authorizations = @Authorization(value = "basic"))
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getAllSnapshotsInfo() throws PipelineException {
    RestAPIUtils.injectPipelineInMDC("*");
    List<SnapshotInfo> snapshotInfoList = new ArrayList<>();
    for(PipelineState pipelineState: manager.getPipelines()) {
      if (!pipelineState.getExecutionMode().equals(ExecutionMode.EDGE)) {
        Runner runner = manager.getRunner(pipelineState.getPipelineId(), pipelineState.getRev());
        if(runner != null) {
          snapshotInfoList.addAll(runner.getSnapshotsInfo());
        }
      }
    }
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapSnapshotInfoNewAPI(
      snapshotInfoList)).build();
  }

  @Path("/pipeline/{pipelineId}/snapshots")
  @GET
  @ApiOperation(value = "Returns Snapshot Info for the given pipeline", response = SnapshotInfoJson.class,
    responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getSnapshotsInfo(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Runner runner = manager.getRunner(pipelineId, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapSnapshotInfoNewAPI(
        runner.getSnapshotsInfo())).build();
    }
    return Response.noContent().build();
  }

  @Path("/pipeline/{pipelineId}/snapshot/{snapshotName}/status")
  @GET
  @ApiOperation(value = "Return Snapshot status", response = SnapshotInfoJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getSnapshotStatus(
    @PathParam("pipelineId") String pipelineId,
    @PathParam("snapshotName") String snapshotName,
    @QueryParam("rev") @DefaultValue("0") String rev
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Runner runner = manager.getRunner(pipelineId, rev);
    if(runner != null) {
      final SnapshotInfoJson snapshot = BeanHelper.wrapSnapshotInfoNewAPI(runner.getSnapshot(snapshotName).getInfo());
      if (snapshot != null) {
        return Response.ok().type(MediaType.APPLICATION_JSON).entity(snapshot).build();
      }
    }
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @Path("/pipeline/{pipelineId}/snapshot/{snapshotName}")
  @GET
  @ApiOperation(value = "Return Snapshot data", response = SnapshotDataJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getSnapshot(
      @PathParam("pipelineId") String pipelineId,
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("attachment") @DefaultValue("false") Boolean attachment
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Runner runner = manager.getRunner(pipelineId, rev);
    if(runner != null) {
      if (attachment) {
        String fileName = pipelineId + "_" + snapshotName;
        return Response.ok().
            header("Content-Disposition", "attachment; filename=\"" + fileName + ".json\"").
            type(MediaType.APPLICATION_JSON).entity(runner.getSnapshot(snapshotName).getOutput()).build();
      } else {
        return Response.ok().type(MediaType.APPLICATION_JSON).entity(runner.getSnapshot(snapshotName).getOutput()).build();
      }
    }
    return Response.noContent().build();
  }

  @Path("/pipeline/{pipelineId}/snapshot/{snapshotName}")
  @DELETE
  @ApiOperation(value = "Delete Snapshot data", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response deleteSnapshot(
      @PathParam("pipelineId") String pipelineId,
      @PathParam("snapshotName") String snapshotName,
      @QueryParam("rev") @DefaultValue("0") String rev
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Runner runner = manager.getRunner(pipelineId, rev);
    if(runner != null) {
      runner.deleteSnapshot(snapshotName);
    }
    return Response.ok().build();
  }

  @Path("/pipeline/{pipelineId}/history")
  @DELETE
  @ApiOperation(value = "Delete history by pipeline name", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response deleteHistory(
    @PathParam("pipelineId") String pipelineId,
    @QueryParam("rev") @DefaultValue("0") String rev
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    if (manager.isRemotePipeline(pipelineId, rev)) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "DELETE_HISTORY", pipelineId);
    }
    Runner runner = manager.getRunner(pipelineId, rev);
    if(runner != null) {
      runner.deleteHistory();
    }
    return Response.ok().build();
  }

  @Path("/pipeline/{pipelineId}/errorRecords")
  @GET
  @ApiOperation(value = "Returns error records by stage instance name and size", response = RecordJson.class,
    responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getErrorRecords(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam ("stageInstanceName") @DefaultValue("") String stageInstanceName,
      @QueryParam ("size") @DefaultValue("10") int size,
      @QueryParam ("edge") boolean edge
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    size = size > 100 ? 100 : size;
    if (edge) {
      PipelineConfiguration pipelineConfiguration = store.load(pipelineId, "0");
      Config edgeHttpUrlConfig = pipelineConfiguration.getConfiguration(EdgeUtil.EDGE_HTTP_URL);
      if (edgeHttpUrlConfig != null) {
        Map<String, Object> params = new HashMap<>();
        params.put("stageInstanceName", stageInstanceName);
        params.put("size", size);
        return EdgeUtil.proxyRequestGET(
            (String)edgeHttpUrlConfig.getValue(),
            "/rest/v1/pipeline/" + pipelineId + "/errorRecords",
            params
        );
      }
    }
    Runner runner = manager.getRunner(pipelineId, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapRecords(runner.getErrorRecords(stageInstanceName, size))).build();
    }
    return Response.noContent().build();
  }

  @Path("/pipeline/{pipelineId}/errorMessages")
  @GET
  @ApiOperation(value = "Returns error messages by stage instance name and size", response = ErrorMessageJson.class,
   responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getErrorMessages(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam ("stageInstanceName") @DefaultValue("") String stageInstanceName,
      @QueryParam ("size") @DefaultValue("10") int size,
      @QueryParam ("edge") boolean edge
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    size = size > 100 ? 100 : size;

    if (edge) {
      PipelineConfiguration pipelineConfiguration = store.load(pipelineId, "0");
      Config edgeHttpUrlConfig = pipelineConfiguration.getConfiguration(EdgeUtil.EDGE_HTTP_URL);
      if (edgeHttpUrlConfig != null) {
        Map<String, Object> params = new HashMap<>();
        params.put("stageInstanceName", stageInstanceName);
        params.put("size", size);
        return EdgeUtil.proxyRequestGET(
            (String)edgeHttpUrlConfig.getValue(),
            "/rest/v1/pipeline/" + pipelineId + "/errorMessages",
            params
        );
      }
    }

    Runner runner = manager.getRunner(pipelineId, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapErrorMessages(runner.getErrorMessages(stageInstanceName, size))).build();
    }
    return Response.noContent().build();
  }

  @Path("/pipeline/{pipelineId}/history")
  @GET
  @ApiOperation(value = "Find history by pipeline name", response = PipelineStateJson.class, responseContainer = "List",
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getHistory(
    @PathParam("pipelineId") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam("fromBeginning") @DefaultValue("false") boolean fromBeginning) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(name);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Runner runner = manager.getRunner(name, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapPipelineStatesNewAPI(runner.getHistory(), false)).build();
    }
    return Response.noContent().build();
  }

  @Path("/pipeline/{pipelineId}/sampledRecords")
  @GET
  @ApiOperation(value = "Returns Sampled records by sample ID and size", response = SampledRecordJson.class,
    responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response getSampledRecords(
    @PathParam("pipelineId") String pipelineId,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam ("sampleId") String sampleId,
    @QueryParam ("sampleSize") @DefaultValue("10") int sampleSize
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    sampleSize = sampleSize > 100 ? 100 : sampleSize;
    Runner runner = manager.getRunner(pipelineId, rev);
    if(runner != null) {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapSampledRecords(runner.getSampledRecords(sampleId, sampleSize))).build();
    }
    return Response.noContent().build();
  }

  @Path("/pipelines/alerts")
  @GET
  @ApiOperation(value = "Returns alerts triggered for all pipelines", response = AlertInfoJson.class,
    responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @PermitAll
  public Response getAllAlerts()
    throws PipelineException {
    RestAPIUtils.injectPipelineInMDC("*");
    List<AlertInfo> alertInfoList = new ArrayList<>();
    if (store.getPipelines().size() < 100) {
      // get alerts for all pipelines only if number of pipelines is less than 100
      for(PipelineState pipelineState: manager.getPipelines()) {
        if (pipelineState.getExecutionMode() != ExecutionMode.EDGE) {
          Runner runner = manager.getRunner(pipelineState.getPipelineId(), pipelineState.getRev());
          if(runner != null && runner.getState().getStatus().isActive()) {
            alertInfoList.addAll(runner.getAlerts());
          }
        }
      }
    }
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapAlertInfoList(
      alertInfoList)).build();
  }


  @Path("/pipeline/{pipelineId}/alerts")
  @DELETE
  @ApiOperation(value = "Delete alert by Pipeline name, revision and Alert ID", response = Boolean.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response deleteAlert(
    @PathParam("pipelineId") String pipelineId,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam("alertId") String alertId
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      manager.getRunner(pipelineId, rev).deleteAlert(alertId)).build();
  }

}
