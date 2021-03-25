/*
 * Copyright 2020 StreamSets Inc.
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
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.execution.CommitPipelineJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ControlHubUtil;
import com.streamsets.datacollector.util.PipelineConfigurationUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.lib.security.http.DpmClientInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

@Path("/v1/controlHub")
@Api(value = "controlHub")
@DenyAll
@RequiresCredentialsDeployed
public class ControlHubResource {

  private final PipelineStoreTask store;
  private final StageLibraryTask stageLibrary;
  private final CredentialStoresTask credentialStoresTask;
  private final Configuration config;
  private final RuntimeInfo runtimeInfo;
  private final BuildInfo buildInfo;
  private final String user;

  @Inject
  public ControlHubResource(
      Configuration config,
      BuildInfo buildInfo,
      RuntimeInfo runtimeInfo,
      StageLibraryTask stageLibrary,
      CredentialStoresTask credentialStoresTask,
      PipelineStoreTask store,
      Principal principal
  ) {
    this.config = config;
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    this.stageLibrary = stageLibrary;
    this.credentialStoresTask = credentialStoresTask;
    this.store = store;
    this.user = principal.getName();
  }

  public DpmClientInfo getDpmClientInfo() {
    return runtimeInfo.getAttribute(DpmClientInfo.RUNTIME_INFO_ATTRIBUTE_KEY);
  }

  private void checkIfControlHubEnabled() {
    if (!runtimeInfo.isDPMEnabled()) {
      throw new RuntimeException("This operation is supported only when Control Hub is enabled");
    }
  }

  @Path("/publishPipeline/{pipelineId}")
  @POST
  @ApiOperation(
      value = "Publish pipeline to Control Hub",
      response = Map.class,
      authorizations = @Authorization(value = "admin")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.ADMIN,
      AuthzRole.ADMIN_REMOTE,
      AuthzRole.CREATOR,
      AuthzRole.CREATOR_REMOTE
  })
  public Response publishPipeline(
      @Context HttpServletRequest request,
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("commitMessage") String commitMessage
  ) throws PipelineException, IOException {
    checkIfControlHubEnabled();
    PipelineConfiguration pipelineConfiguration = store.load(pipelineId, "0");
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(
        stageLibrary,
        buildInfo,
        pipelineId,
        pipelineConfiguration,
        user,
        new HashMap<>()
    );
    pipelineConfiguration = validator.validate();
    RuleDefinitions ruleDefinitions = store.retrieveRules(pipelineId, "0");
    PipelineEnvelopeJson pipelineEnvelopeJson = PipelineConfigurationUtil.getPipelineEnvelope(
        stageLibrary,
        credentialStoresTask,
        config,
        pipelineConfiguration,
        ruleDefinitions,
        true,
        true
    );
    ObjectMapper objectMapper = ObjectMapperFactory.get();
    CommitPipelineJson commitPipelineModel = new CommitPipelineJson();
    commitPipelineModel.setName(pipelineConfiguration.getTitle());
    commitPipelineModel.setCommitMessage(commitMessage);
    commitPipelineModel.setPipelineDefinition(
        objectMapper.writeValueAsString(pipelineEnvelopeJson.getPipelineConfig())
    );
    commitPipelineModel.setRulesDefinition(
        objectMapper.writeValueAsString(pipelineEnvelopeJson.getPipelineRules())
    );
    commitPipelineModel.setLibraryDefinitions(
        objectMapper.writeValueAsString(pipelineEnvelopeJson.getLibraryDefinitions())
    );
    PipelineConfigurationJson publishedPipeline = ControlHubUtil.publishPipeline(
        request, getDpmClientInfo(),
        commitPipelineModel
    );

    // update metadata
    Map<String, Object> metadata = publishedPipeline.getMetadata();
    metadata.put("lastConfigId", pipelineConfiguration.getUuid());
    metadata.put("lastRulesId", ruleDefinitions.getUuid());
    PipelineConfiguration updatePipeline = store.saveMetadata(user, pipelineId, "0", metadata);

    return Response.status(Response.Status.OK)
        .entity(BeanHelper.wrapPipelineConfiguration(updatePipeline))
        .build();
  }

  @Path("/pipelines")
  @GET
  @ApiOperation(
      value = "Get Pipelines from Control Hub",
      response = Map.class,
      authorizations = @Authorization(value = "guest")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelines(
      @Context HttpServletRequest request,
      @QueryParam("offset") @DefaultValue("0") int offset,
      @QueryParam("len") @DefaultValue("50") int len,
      @QueryParam("executionModes") String executionModes
  ) {
    checkIfControlHubEnabled();
    return ControlHubUtil.getPipelines(request, getDpmClientInfo(), offset, len, executionModes);
  }

  @Path("/pipeline/{pipelineCommitId}")
  @GET
  @ApiOperation(
      value = "Get Pipeline from Control Hub for a given pipeline commitId",
      response = Map.class,
      authorizations = @Authorization(value = "guest")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipeline(
      @Context HttpServletRequest request,
      @PathParam("pipelineCommitId") String pipelineCommitId
  ) {
    checkIfControlHubEnabled();
    return ControlHubUtil.getPipeline(request, getDpmClientInfo(), pipelineCommitId);
  }

  @Path("/pipeline/{pipelineId}/log")
  @GET
  @ApiOperation(
      value = "Get Pipeline Commit History from Control Hub for a given pipeline Id",
      response = Map.class,
      authorizations = @Authorization(value = "guest")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelineCommitHistory(
      @Context HttpServletRequest request,
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("offset") @DefaultValue("0") int offset,
      @QueryParam("len") @DefaultValue("50") int len,
      @QueryParam("order") String order
  ) {
    checkIfControlHubEnabled();
    return ControlHubUtil.getPipelineCommitHistory(
        request, getDpmClientInfo(),
        pipelineId,
        offset,
        len,
        order
    );
  }

  @Path("/currentUser")
  @GET
  @ApiOperation(
      value = "Get Control Hub User Roles",
      response = Map.class,
      authorizations = @Authorization(value = "guest")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getRemoteRoles(@Context HttpServletRequest request) {
    checkIfControlHubEnabled();
    return ControlHubUtil.getRemoteRoles(request, getDpmClientInfo());
  }

  @Path("/users")
  @GET
  @ApiOperation(
      value = "Get Control Hub Users",
      response = Map.class,
      authorizations = @Authorization(value = "guest")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getControlHubUsers(
      @Context HttpServletRequest request,
      @QueryParam("offset") @DefaultValue("0") int offset,
      @QueryParam("len") @DefaultValue("50") int len
  ) {
    checkIfControlHubEnabled();
    return ControlHubUtil.getControlHubUsers(request, getDpmClientInfo(), offset, len);
  }

  @Path("/groups")
  @GET
  @ApiOperation(
      value = "Get Control Hub Groups",
      response = Map.class,
      authorizations = @Authorization(value = "guest")
  )
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getControlHubGroups(
      @Context HttpServletRequest request,
      @QueryParam("offset") @DefaultValue("0") int offset,
      @QueryParam("len") @DefaultValue("50") int len
  ) {
    checkIfControlHubEnabled();
    return ControlHubUtil.getControlHubGroups(request, getDpmClientInfo(), offset, len);
  }

}
