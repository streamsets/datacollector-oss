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

import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.execution.AclManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.RawPreview;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PreviewInfoJson;
import com.streamsets.datacollector.restapi.bean.PreviewOutputJson;
import com.streamsets.datacollector.restapi.bean.StageOutputJson;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.EdgeUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.pipeline.api.Config;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v1")
@Api(value = "preview")
@DenyAll
@RequiresCredentialsDeployed
public class PreviewResource {
  private static final String MAX_BATCH_SIZE_KEY = "preview.maxBatchSize";
  private static final int MAX_BATCH_SIZE_DEFAULT = 10;
  private static final String MAX_BATCHES_KEY = "preview.maxBatches";
  private static final int MAX_BATCHES_DEFAULT = 10;

  private final Manager manager;
  private final PipelineStoreTask store;
  private final Configuration configuration;
  private final String user;

  @Inject
  public PreviewResource(
      Manager manager,
      Configuration configuration,
      Principal principal,
      PipelineStoreTask store,
      AclStoreTask aclStore,
      RuntimeInfo runtimeInfo,
      UserGroupManager userGroupManager
  ) {
    this.configuration = configuration;
    this.user = principal.getName();
    this.store = store;

    UserJson currentUser;
    if (runtimeInfo.isDPMEnabled()) {
      currentUser = new UserJson((SSOPrincipal)principal);
    } else {
      currentUser = userGroupManager.getUser(principal);
    }

    if (runtimeInfo.isAclEnabled()) {
      this.manager = new AclManager(manager, aclStore, currentUser);
    } else {
      this.manager = manager;
    }
  }

  @Path("/pipeline/{pipelineId}/preview")
  @POST
  @ApiOperation(value = "Run Pipeline preview",
      response = PreviewInfoJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.MANAGER,
      AuthzRole.MANAGER_REMOTE
  })
  public Response previewWithOverride(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") String rev,
      @QueryParam("batchSize") @DefaultValue("" + Integer.MAX_VALUE) int batchSize,
      @QueryParam("batches") @DefaultValue("1") int batches,
      @QueryParam("skipTargets") @DefaultValue("true") boolean skipTargets,
      @QueryParam("skipLifecycleEvents") @DefaultValue("true") boolean skipLifecycleEvents,
      @QueryParam("endStage") String endStageInstanceName,
      @QueryParam("timeout") @DefaultValue("2000") long timeout,
      @QueryParam("edge") @DefaultValue("false") boolean edge,
      @QueryParam("testOrigin") @DefaultValue("false") boolean testOrigin,
      @ApiParam(name="stageOutputsToOverrideJson", required = true)  List<StageOutputJson> stageOutputsToOverrideJson
  ) throws PipelineException {
    if (stageOutputsToOverrideJson == null) {
      stageOutputsToOverrideJson = Collections.emptyList();
    }

    if (edge) {
      PipelineConfiguration pipelineConfiguration = store.load(pipelineId, "0");
      Config edgeHttpUrlConfig = pipelineConfiguration.getConfiguration(EdgeUtil.EDGE_HTTP_URL);
      if (edgeHttpUrlConfig != null) {
        EdgeUtil.publishEdgePipeline(pipelineConfiguration, null);
        Map<String, Object> params = new HashMap<>();
        params.put("bathces", batches);
        params.put("batchSize", batchSize);
        params.put("skipTargets", skipTargets);
        params.put("endStage", endStageInstanceName);
        params.put("timeout", timeout);
        params.put("testOrigin", testOrigin);
        return EdgeUtil.proxyRequestPOST(
            (String)edgeHttpUrlConfig.getValue(),
            "/rest/v1/pipeline/" + pipelineId + "/preview",
            params,
            stageOutputsToOverrideJson
        );
      }
    }

    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    batchSize = Math.min(maxBatchSize, batchSize);
    int maxBatches = configuration.get(MAX_BATCHES_KEY, MAX_BATCHES_DEFAULT);
    batches = Math.min(maxBatches, batches);

    Previewer previewer = manager.createPreviewer(this.user, pipelineId, rev);
    try {
      previewer.start(
          batches,
          batchSize,
          skipTargets,
          skipLifecycleEvents,
          endStageInstanceName,
          BeanHelper.unwrapStageOutput(stageOutputsToOverrideJson),
          timeout,
          testOrigin
      );
      PreviewInfoJson previewInfoJson = new PreviewInfoJson(previewer.getId(), previewer.getStatus());
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewInfoJson).build();
    } catch (PipelineRuntimeException ex) {
      if (ex.getErrorCode() == ContainerError.CONTAINER_0165) {
        return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(
            BeanHelper.wrapIssues(ex.getIssues())).build();
      } else {
        throw ex;
      }
    }
  }

  @Path("/pipeline/{pipelineId}/preview/{previewerId}/status")
  @GET
  @ApiOperation(value = "Return Preview status by previewer ID", response = PreviewInfoJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.MANAGER,
      AuthzRole.MANAGER_REMOTE
  })
  public Response getPreviewStatus(
      @PathParam("pipelineId") String pipelineId,
      @PathParam("previewerId") String previewerId,
      @QueryParam("edge") @DefaultValue("false") boolean edge
  ) throws PipelineException {
    if (edge) {
      PipelineConfiguration pipelineConfiguration = store.load(pipelineId, "0");
      Config edgeHttpUrlConfig = pipelineConfiguration.getConfiguration(EdgeUtil.EDGE_HTTP_URL);
      if (edgeHttpUrlConfig != null) {
        return EdgeUtil.proxyRequestGET(
            (String)edgeHttpUrlConfig.getValue(),
            "/rest/v1/pipeline/" + pipelineId + "/preview/" + previewerId + "/status",
            Collections.emptyMap()
        );
      }
    }
    Previewer previewer = manager.getPreviewer(previewerId);
    if(previewer == null) {
      return Response.status(Response.Status.NOT_FOUND).entity("Cannot find previewer with id " + previewerId).build();
    }
    PipelineInfo pipelineInfo = store.getInfo(previewer.getName());
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    PreviewInfoJson previewInfoJson = new PreviewInfoJson(previewer.getId(), previewer.getStatus());
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewInfoJson).build();
  }

  @Path("/pipeline/{pipelineId}/preview/{previewerId}")
  @GET
  @ApiOperation(value = "Return Preview Data by previewer ID", response = PreviewOutputJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.MANAGER,
      AuthzRole.MANAGER_REMOTE
  })
  public Response getPreviewData(
      @PathParam("pipelineId") String pipelineId,
      @PathParam("previewerId") String previewerId,
      @QueryParam("edge") @DefaultValue("false") boolean edge
  ) throws PipelineException {
    if (edge) {
      PipelineConfiguration pipelineConfiguration = store.load(pipelineId, "0");
      Config edgeHttpUrlConfig = pipelineConfiguration.getConfiguration(EdgeUtil.EDGE_HTTP_URL);
      if (edgeHttpUrlConfig != null) {
        return EdgeUtil.proxyRequestGET(
            (String)edgeHttpUrlConfig.getValue(),
            "/rest/v1/pipeline/" + pipelineId + "/preview/" + previewerId,
            Collections.emptyMap()
        );
      }
    }
    Previewer previewer = manager.getPreviewer(previewerId);
    if(previewer == null) {
      return Response.status(Response.Status.NOT_FOUND).entity("Cannot find previewer with id " + previewerId).build();
    }
    PipelineInfo pipelineInfo = store.getInfo(previewer.getName());
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    PreviewOutput previewOutput = previewer.getOutput();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapPreviewOutput(previewOutput)).build();
  }

  @Path("/pipeline/{pipelineId}/preview/{previewerId}")
  @DELETE
  @ApiOperation(value = "Stop Preview by previewer ID", response = PreviewInfoJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.MANAGER,
      AuthzRole.MANAGER_REMOTE
  })
  public Response stopPreview(
      @PathParam("pipelineId") String pipelineId,
      @PathParam("previewerId") String previewerId,
      @QueryParam("edge") @DefaultValue("false") boolean edge
  ) throws PipelineException {
    if (edge) {
      PipelineConfiguration pipelineConfiguration = store.load(pipelineId, "0");
      Config edgeHttpUrlConfig = pipelineConfiguration.getConfiguration(EdgeUtil.EDGE_HTTP_URL);
      if (edgeHttpUrlConfig != null) {
        return EdgeUtil.proxyRequestDELETE(
            (String)edgeHttpUrlConfig.getValue(),
            "/rest/v1/pipeline/" + pipelineId + "/preview/" + previewerId,
            Collections.emptyMap()
        );
      }
    }
    Previewer previewer = manager.getPreviewer(previewerId);
    if(previewer == null) {
      return Response.status(Response.Status.NOT_FOUND).entity("Cannot find previewer with id " + previewerId).build();
    }
    PipelineInfo pipelineInfo = store.getInfo(previewer.getName());
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    previewer.stop();
    PreviewInfoJson previewInfoJson = new PreviewInfoJson(previewer.getId(), previewer.getStatus());
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewInfoJson).build();
  }

  @Path("/pipeline/{pipelineId}/rawSourcePreview")
  @GET
  @ApiOperation(value = "Get raw source preview data for pipeline name and revision", response = RawPreview.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE, AuthzRole.MANAGER, AuthzRole.MANAGER_REMOTE
  })
  public Response rawSourcePreview(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") String rev,
      @Context UriInfo uriInfo
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    MultivaluedMap<String, String> previewParams = uriInfo.getQueryParameters();
    Previewer previewer = manager.createPreviewer(this.user, pipelineId, rev);
    RawPreview rawPreview = previewer.getRawSource(4 * 1024, previewParams);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(rawPreview).build();
  }

  @Path("/pipeline/{pipelineId}/validate")
  @GET
  @ApiOperation(value = "Validate pipeline configuration and return validation status and issues",
      response = PreviewInfoJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response validateConfigs(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") String rev,
      @QueryParam("timeout") @DefaultValue("2000") long timeout,
      @QueryParam("edge") @DefaultValue("false") boolean edge
  ) throws PipelineException {
    if (edge) {
      PipelineConfiguration pipelineConfiguration = store.load(pipelineId, "0");
      Config edgeHttpUrlConfig = pipelineConfiguration.getConfiguration(EdgeUtil.EDGE_HTTP_URL);
      if (edgeHttpUrlConfig != null) {
        EdgeUtil.publishEdgePipeline(pipelineConfiguration, null);
        return EdgeUtil.proxyRequestGET(
            (String)edgeHttpUrlConfig.getValue(),
            "/rest/v1/pipeline/" + pipelineId + "/validate",
            ImmutableMap.of("timeout", timeout)
        );
      }
    }
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    try {
      Previewer previewer = manager.createPreviewer(this.user, pipelineId, rev);
      previewer.validateConfigs(timeout);
      PreviewStatus previewStatus = previewer.getStatus();
      if(previewStatus == null) {
        previewStatus =  PreviewStatus.VALIDATING;
      }
      PreviewInfoJson previewInfoJson = new PreviewInfoJson(previewer.getId(), previewStatus);
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewInfoJson).build();
    } catch (PipelineRuntimeException ex) {
      if (ex.getErrorCode() == ContainerError.CONTAINER_0165) {
        return Response.status(Response.Status.BAD_REQUEST).type(MediaType.APPLICATION_JSON).entity(
            BeanHelper.wrapIssues(ex.getIssues())).build();
      } else {
        throw ex;
      }
    }
  }

}
