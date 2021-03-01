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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.dynamicpreview.DynamicPreviewConstants;
import com.streamsets.datacollector.dynamicpreview.DynamicPreviewRequestJson;
import com.streamsets.datacollector.dynamicpreview.DynamicPreviewType;
import com.streamsets.datacollector.event.binding.MessagingDtoJsonMapper;
import com.streamsets.datacollector.event.binding.MessagingJsonToFromDto;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.dto.PipelinePreviewEvent;
import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollectorResult;
import com.streamsets.datacollector.event.json.DynamicPreviewEventJson;
import com.streamsets.datacollector.event.json.EventJson;
import com.streamsets.datacollector.event.json.customdeserializer.DynamicPreviewEventDeserializer;
import com.streamsets.datacollector.execution.AclManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PreviewOutput;
import com.streamsets.datacollector.execution.PreviewStatus;
import com.streamsets.datacollector.execution.Previewer;
import com.streamsets.datacollector.execution.RawPreview;
import com.streamsets.datacollector.execution.preview.common.PreviewError;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.ConnectionDefinitionPreviewJson;
import com.streamsets.datacollector.restapi.bean.DynamicPreviewRequestWithOverridesJson;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.PreviewInfoJson;
import com.streamsets.datacollector.restapi.bean.PreviewOutputJson;
import com.streamsets.datacollector.restapi.bean.StageOutputJson;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.restapi.connection.ConnectionVerifierDynamicPreviewHelper;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.EdgeUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.ForbiddenException;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.RestClient;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.DenyAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
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
import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v1")
@Api(value = "preview")
@DenyAll
@RequiresCredentialsDeployed
public class PreviewResource {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewResource.class);
  public static final String MAX_BATCH_SIZE_KEY = "preview.maxBatchSize";
  public static final int MAX_BATCH_SIZE_DEFAULT = 1000;
  public static final String MAX_BATCHES_KEY = "preview.maxBatches";
  public static final int MAX_BATCHES_DEFAULT = 10;

  //TODO: look into avoiding duplicating constants with DPM (com.streamsets.apps.common.Roles and ClassificationRoles)
  private static final List<String> DYNAMIC_PREVIEW_ALLOWED_ROLES_CLASSIFICATION = Arrays.asList(
      // classification administrator (SDP specific role)
      "classification:admin",
      // system administrator (DPM common role)
      "sys-admin",
      // organization administrator (DPM common role)
      "org-admin"
  );

  private static final int DYNAMIC_PREVIEW_MAX_COUNT = 100;

  private final Manager manager;
  private final PipelineStoreTask store;
  private final Configuration configuration;
  private final String user;
  private final EventHandlerTask eventHandlerTask;
  private final PipelineStoreTask pipelineStoreTask;
  private final StageLibraryTask stageLibraryTask;
  private final BlobStoreTask blobStoreTask;
  private final RuntimeInfo runtimeInfo;
  private final UserJson currentUser;

  @Inject
  public PreviewResource(
      Manager manager,
      Configuration configuration,
      Principal principal,
      PipelineStoreTask store,
      AclStoreTask aclStore,
      RuntimeInfo runtimeInfo,
      UserGroupManager userGroupManager,
      EventHandlerTask eventHandlerTask,
      PipelineStoreTask pipelineStoreTask,
      BlobStoreTask blobStoreTask,
      StageLibraryTask stageLibraryTask
  ) {
    this.configuration = configuration;
    this.user = principal.getName();
    this.store = store;
    this.eventHandlerTask = eventHandlerTask;
    this.pipelineStoreTask = pipelineStoreTask;
    this.blobStoreTask = blobStoreTask;
    this.stageLibraryTask = stageLibraryTask;
    PipelineBeanCreator.prepareForConnections(configuration, runtimeInfo);

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

    this.runtimeInfo = runtimeInfo;
  }

  @Path("/pipeline/{pipelineId}/preview")
  @POST
  @ApiOperation(value = "Run Pipeline preview",
      response = PreviewInfoJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
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
      @QueryParam("remote") @DefaultValue("false") boolean remote,
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

    return startPreviewer(
        pipelineId,
        rev,
        batchSize,
        batches,
        skipTargets,
        skipLifecycleEvents,
        endStageInstanceName,
        timeout,
        testOrigin,
        remote,
        stageOutputsToOverrideJson
    );
  }

  private Response startPreviewer(
      String pipelineId,
      String rev,
      int batchSize,
      int batches,
      boolean skipTargets,
      boolean skipLifecycleEvents,
      String endStageInstanceName,
      long timeout,
      boolean testOrigin,
      boolean remote,
      List<StageOutputJson> stageOutputsToOverrideJson
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    int maxBatchSize = configuration.get(MAX_BATCH_SIZE_KEY, MAX_BATCH_SIZE_DEFAULT);
    batchSize = Math.min(maxBatchSize, batchSize);
    int maxBatches = configuration.get(MAX_BATCHES_KEY, MAX_BATCHES_DEFAULT);
    batches = Math.min(maxBatches, batches);

    Previewer previewer = manager.createPreviewer(this.user, pipelineId, rev, Collections.emptyList(), p -> null, remote, new HashMap<>());
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
      PreviewInfoJson previewInfoJson = new PreviewInfoJson(
          previewer.getId(),
          previewer.getStatus(),
          pipelineId,
          previewer.getAttributes()
      );
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

  @Path("/pipeline/dynamicPreview")
  @POST
  @ApiOperation(
      value = "Run dynamic pipeline preview",
      response = PreviewInfoJson.class,
      authorizations = @Authorization(value = "basic")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.MANAGER,
      AuthzRole.MANAGER_REMOTE
  })
  public Response initiateDynamicPreview(
      @ApiParam(
          name="dynamicPreviewRequest",
          required = true
      ) DynamicPreviewRequestWithOverridesJson dynamicPreviewWithOverridesRequest
  ) throws StageException, IOException {
    Utils.checkNotNull(dynamicPreviewWithOverridesRequest, "dynamicPreviewWithOverridesRequest");
    final DynamicPreviewRequestJson dynamicPreviewRequest =
        dynamicPreviewWithOverridesRequest.getDynamicPreviewRequestJson();

    Utils.checkNotNull(dynamicPreviewRequest, "dynamicPreviewRequestJson");

    checkDynamicPreviewPermissions(dynamicPreviewRequest);

    // TODO: fix this hack?
    final String orgId = StringUtils.substringAfterLast(currentUser.getName(), "@");
    final Map<String, Object> params = dynamicPreviewRequest.getParameters();
    params.put(DynamicPreviewConstants.REQUESTING_USER_ID_PARAMETER, currentUser.getName());
    params.put(DynamicPreviewConstants.REQUESTING_USER_ORG_ID_PARAMETER, orgId);

    final String controlHubBaseUrl = RemoteSSOService.getValidURL(configuration.get(
        RemoteSSOService.DPM_BASE_URL_CONFIG,
        RemoteSSOService.DPM_BASE_URL_DEFAULT
    ));

    // serialize the stage overrides to a String, to be passed to Control Hub (see Javadocs in DynamicPreviewRequestJson
    // for a more complete explanation)
    dynamicPreviewRequest.setStageOutputsToOverrideJsonText(ObjectMapperFactory.get().writeValueAsString(
        dynamicPreviewWithOverridesRequest.getStageOutputsToOverrideJson()
    ));

    DynamicPreviewEventJson dynamicPreviewEvent;
    Map<String, ConnectionConfiguration> connections = new HashMap<>();
    if (DynamicPreviewType.CONNECTION_VERIFIER.equals(dynamicPreviewRequest.getType())) {
      // build the dynamic preview pipeline and events
      ConnectionVerifierDynamicPreviewHelper verifierHelper = new ConnectionVerifierDynamicPreviewHelper(stageLibraryTask);
      ConnectionDefinitionPreviewJson connection = verifierHelper.getConnectionPreviewJson(dynamicPreviewRequest);
      PipelineEnvelopeJson verifierPipeline = verifierHelper.getVerifierDynamicPreviewPipeline(connection);
      dynamicPreviewEvent = verifierHelper.getVerifierDynamicPreviewEvent(verifierPipeline, dynamicPreviewRequest, currentUser);
      connections = verifierHelper.getVerifierConfigurationJson(connection);
    } else {
      // for classification and protection policies get the preview event from SCH
      dynamicPreviewEvent = getDynamicPreviewEvent(
          controlHubBaseUrl,
          "/dynamic_preview/rest/v2/dynamic_preview/createDynamicPreviewEvent",
          runtimeInfo.getAppAuthToken(),
          runtimeInfo.getId(),
          dynamicPreviewRequest
      );
    }

    String generatedPipelineId;
    try {
      generatedPipelineId = handlePreviewEvents(
          "before",
          dynamicPreviewEvent.getBeforeActionsEventTypeIds(),
          dynamicPreviewEvent.getBeforeActions(),
          connections
      );
    } catch (IOException e) {
      throw new StageException(PreviewError.PREVIEW_0101, "before", e.getMessage(), e);
    }

    if (generatedPipelineId == null) {
      throw new StageException(PreviewError.PREVIEW_0103);
    }

    final PipelinePreviewEvent previewEvent =
        MessagingDtoJsonMapper.INSTANCE.asPipelinePreviewEventDto(dynamicPreviewEvent.getPreviewEvent());

    // set up after actions
    previewEvent.setAfterActionsFunction(p -> {
      LOG.debug("Running after actions for dynamic preview");
      try {
        handlePreviewEvents(
            "after",
            dynamicPreviewEvent.getAfterActionsEventTypeIds(),
            dynamicPreviewEvent.getAfterActions(),
            new HashMap<>()
        );
      } catch (IOException e) {
        LOG.error(
            "IOException attempting to handle after actions for dynamic preview: {}",
            e.getMessage(),
            e
        );
      }
      return null;
    });


    final RemoteDataCollectorResult previewEventResult = eventHandlerTask.handleLocalEvent(
        previewEvent,
        EventType.fromValue(dynamicPreviewEvent.getPreviewEventTypeId()),
        connections
    );

    String previewerId;
    if (previewEventResult.getImmediateResult() == null) {
      throw new StageException(PreviewError.PREVIEW_0102);
    } else {
      previewerId = (String) previewEventResult.getImmediateResult();
    }
    final Previewer previewer = manager.getPreviewer(previewerId);
    if (previewer == null) {
      throw new StageException(PreviewError.PREVIEW_0105);
    }

    final PreviewInfoJson previewInfoJson = new PreviewInfoJson(
        previewer.getId(),
        previewer.getStatus(),
        generatedPipelineId,
        previewer.getAttributes()
    );
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(previewInfoJson).build();
  }

  private void checkDynamicPreviewPermissions(DynamicPreviewRequestJson dynamicPreviewRequest) {
    switch (dynamicPreviewRequest.getType()) {
      case CLASSIFICATION_CATALOG:
        if (!CollectionUtils.containsAny(currentUser.getRoles(), DYNAMIC_PREVIEW_ALLOWED_ROLES_CLASSIFICATION)) {
          throw new ForbiddenException(Collections.singletonMap("message", String.format(
              "User did not have any roles that would allow performing %s type dynamic preview",
              DynamicPreviewType.CLASSIFICATION_CATALOG.name()
          )));
        }
        break;
      case PROTECTION_POLICY:
        // permissions on specific policies will be checked on DPM side
        break;
      case CONNECTION_VERIFIER:
        // if the user could run the preview then it should have the appropriate roles
        break;
    }
  }

  private static final ObjectMapper DYNAMIC_PREVIEW_REQUEST_OBJECT_MAPPER;
  static {
    DYNAMIC_PREVIEW_REQUEST_OBJECT_MAPPER = new ObjectMapper();
    final SimpleModule module = new SimpleModule();
    module.addDeserializer(DynamicPreviewEventJson.class, new DynamicPreviewEventDeserializer());
    DYNAMIC_PREVIEW_REQUEST_OBJECT_MAPPER.registerModule(module);
  }

  @NotNull
  private DynamicPreviewEventJson getDynamicPreviewEvent(
      String schBaseUrl,
      String requestPath,
      String appAuthToken,
      String componentId,
      DynamicPreviewRequestJson requestJson
  ) throws IOException {

    final RestClient dynamicPreviewAppClient = RestClient.builder(schBaseUrl).json(true).appAuthToken(appAuthToken)
        .componentId(componentId).csrf(true).jsonMapper(DYNAMIC_PREVIEW_REQUEST_OBJECT_MAPPER).build(requestPath);

    final RestClient.Response response = dynamicPreviewAppClient.post(requestJson);
    if (response.haveData() && response.successful()) {
      return response.getData(DynamicPreviewEventJson.class);
    } else {
      String errorBody = StringUtils.join(response.getError());
      LOG.error(
          "Error trying to get DynamicPreviewEventJson; status {}, error: {}",
          response.getStatus(),
          errorBody
      );
      throw new StageException(PreviewError.PREVIEW_0104, errorBody);
    }
  }

  private String handlePreviewEvents(
      String kind,
      List<Integer> eventTypeIds,
      List<EventJson> events,
      Map<String, ConnectionConfiguration> connections
  ) throws IOException {
    String generatedPipelineId = null;
    if (eventTypeIds == null && events == null) {
      LOG.debug("Null {} events; nothing to do", kind);
      return null;
    }
    if (eventTypeIds.size() != events.size()) {
      throw new IllegalArgumentException(String.format(
          "Length of type IDs (%d) did not match length of events (%d); cannot process %s events in dynamic preview",
          eventTypeIds.size(),
          events.size(),
          kind
      ));
    }
    for (int i=0; i<eventTypeIds.size(); i++) {
      final int eventTypeId = eventTypeIds.get(i);
      final EventType eventType = EventType.fromValue(eventTypeId);
      final RemoteDataCollectorResult result = eventHandlerTask.handleLocalEvent(
          MessagingJsonToFromDto.INSTANCE.asDto(events.get(i), eventTypeId),
          eventType,
          connections
      );
      if (eventType == EventType.SAVE_PIPELINE && result.getImmediateResult() != null) {
        generatedPipelineId = (String) result.getImmediateResult();
      }
    }
    return generatedPipelineId;
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
    PreviewInfoJson previewInfoJson = new PreviewInfoJson(
        previewer.getId(),
        previewer.getStatus(),
        pipelineId,
        previewer.getAttributes()
    );
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
    if (previewer == null) {
      return Response.status(Response.Status.NOT_FOUND).entity("Cannot find previewer with id " + previewerId).build();
    }
    PipelineInfo pipelineInfo = store.getInfo(previewer.getName());
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    previewer.stop();
    PreviewInfoJson previewInfoJson = new PreviewInfoJson(
        previewer.getId(),
        previewer.getStatus(),
        pipelineId,
        previewer.getAttributes()
    );
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
    Previewer previewer = manager.createPreviewer(this.user, pipelineId, rev, Collections.emptyList(), p -> null, false, new HashMap<>());
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
      @QueryParam("edge") @DefaultValue("false") boolean edge,
      @QueryParam("remote") @DefaultValue("false") boolean remote
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
      Previewer previewer = manager.createPreviewer(this.user, pipelineId, rev, Collections.emptyList(), p -> null, remote, new HashMap<>());
      previewer.validateConfigs(timeout);
      PreviewStatus previewStatus = previewer.getStatus();
      if(previewStatus == null) {
        previewStatus =  PreviewStatus.VALIDATING;
      }
      PreviewInfoJson previewInfoJson = new PreviewInfoJson(
          previewer.getId(),
          previewStatus,
          pipelineId,
          previewer.getAttributes()
      );
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
