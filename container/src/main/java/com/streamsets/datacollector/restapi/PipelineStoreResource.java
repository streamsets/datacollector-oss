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
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.DriftRuleDefinition;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.creation.RuleDefinitionsConfigBean;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollector;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.AddLabelsRequestJson;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.DefinitionsJson;
import com.streamsets.datacollector.restapi.bean.MultiStatusResponseJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.PipelineInfoJson;
import com.streamsets.datacollector.restapi.bean.PipelineRulesDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineStateJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.restapi.bean.StageDefinitionJson;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.AclPipelineStoreTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.datacollector.validation.RuleDefinitionValidator;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.impl.Utils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

@Path("/v1")
@Api(value = "store")
@DenyAll
@RequiresCredentialsDeployed
public class PipelineStoreResource {
  private static final String HIGH_BAD_RECORDS_ID = "badRecordsAlertID";
  private static final String HIGH_BAD_RECORDS_TEXT = "High incidence of Error Records";
  private static final String HIGH_BAD_RECORDS_METRIC_ID = "pipeline.batchErrorRecords.counter";
  private static final String HIGH_BAD_RECORDS_CONDITION = "${value() > 100}";

  private static final String HIGH_STAGE_ERRORS_ID = "stageErrorAlertID";
  private static final String HIGH_STAGE_ERRORS_TEXT = "High incidence of Stage Errors";
  private static final String HIGH_STAGE_ERRORS_METRIC_ID = "pipeline.batchErrorMessages.counter";
  private static final String HIGH_STAGE_ERRORS_CONDITION = "${value() > 100}";

  private static final String PIPELINE_IDLE_ID = "idleGaugeID";
  private static final String PIPELINE_IDLE_TEXT = "Pipeline is Idle";
  private static final String PIPELINE_IDLE_METRIC_ID = "RuntimeStatsGauge.gauge";
  private static final String PIPELINE_IDLE_CONDITION = "${time:now() - value() > 120000}";

  private static final String BATCH_TIME_ID = "batchTimeAlertID";
  private static final String BATCH_TIME_TEXT = "Batch taking more time to process";
  private static final String BATCH_TIME_METRIC_ID = "RuntimeStatsGauge.gauge";
  private static final String BATCH_TIME_CONDITION = "${value() > 200}";

  private static final String MEMORY_LIMIt_ID = "memoryLimitAlertID";
  private static final String MEMORY_LIMIt_TEXT = "Memory limit for pipeline exceeded";
  private static final String MEMORY_LIMIt_METRIC_ID = "pipeline.memoryConsumed.counter";
  private static final String MEMORY_LIMIt_CONDITION = "${value() > (jvm:maxMemoryMB() * 0.65)}";

  private static final String DPM_PIPELINE_ID = "dpm.pipeline.id";

  private static final String SYSTEM_ALL_PIPELINES = "system:allPipelines";
  private static final String SYSTEM_PUBLISHED_PIPELINES = "system:publishedPipelines";
  private static final String SYSTEM_DPM_CONTROLLED_PIPELINES = "system:dpmControlledPipelines";
  private static final String SYSTEM_LOCAL_PIPELINES = "system:localPipelines";
  private static final String SYSTEM_RUNNING_PIPELINES = "system:runningPipelines";
  private static final String SYSTEM_NON_RUNNING_PIPELINES = "system:nonRunningPipelines";
  private static final String SYSTEM_INVALID_PIPELINES = "system:invalidPipelines";
  private static final String SYSTEM_ERROR_PIPELINES = "system:errorPipelines";
  private static final String SHARED_WITH_ME_PIPELINES = "system:sharedWithMePipelines";



  private static final List<String> SYSTEM_PIPELINE_LABELS = ImmutableList.of(
      SYSTEM_ALL_PIPELINES,
      SYSTEM_RUNNING_PIPELINES,
      SYSTEM_NON_RUNNING_PIPELINES,
      SYSTEM_INVALID_PIPELINES,
      SYSTEM_ERROR_PIPELINES,
      SHARED_WITH_ME_PIPELINES
  );

  private static final List<String> DPM_ENABLED_SYSTEM_PIPELINE_LABELS = ImmutableList.of(
      SYSTEM_ALL_PIPELINES,
      SYSTEM_PUBLISHED_PIPELINES,
      SYSTEM_DPM_CONTROLLED_PIPELINES,
      SYSTEM_LOCAL_PIPELINES,
      SYSTEM_RUNNING_PIPELINES,
      SYSTEM_NON_RUNNING_PIPELINES,
      SYSTEM_INVALID_PIPELINES,
      SYSTEM_ERROR_PIPELINES,
      SHARED_WITH_ME_PIPELINES
  );

  private static final Logger LOG = LoggerFactory.getLogger(PipelineStoreResource.class);

  private final RuntimeInfo runtimeInfo;
  private final Manager manager;
  private final PipelineStoreTask store;
  private final StageLibraryTask stageLibrary;
  private final URI uri;
  private final String user;

  @Inject
  public PipelineStoreResource(
      URI uri,
      Principal principal,
      StageLibraryTask stageLibrary,
      PipelineStoreTask store,
      RuntimeInfo runtimeInfo,
      Manager manager,
      UserGroupManager userGroupManager,
      AclStoreTask aclStore
  ) {
    this.uri = uri;
    this.user = principal.getName();
    this.stageLibrary = stageLibrary;
    this.runtimeInfo = runtimeInfo;
    this.manager = manager;

    UserJson currentUser;
    if (runtimeInfo.isDPMEnabled()) {
      currentUser = new UserJson((SSOPrincipal)principal);
    } else {
      currentUser = userGroupManager.getUser(principal);
    }

    if (runtimeInfo.isAclEnabled()) {
      this.store = new AclPipelineStoreTask(store, aclStore, currentUser);
    } else {
      this.store = store;
    }
  }

  @Path("/pipelines/count")
  @GET
  @ApiOperation(value = "Returns total Pipelines count", response = Map.class,
      responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelinesCount() throws PipelineStoreException {
    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(ImmutableMap.of("count", store.getPipelines().size()))
        .build();
  }

  @Path("/pipelines/systemLabels")
  @GET
  @ApiOperation(value = "Returns System Pipeline Labels", response = List.class,
      responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getSystemPipelineLabels() throws PipelineStoreException {
    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(runtimeInfo.isDPMEnabled() ? DPM_ENABLED_SYSTEM_PIPELINE_LABELS : SYSTEM_PIPELINE_LABELS)
        .build();
  }

  @Path("/pipelines/labels")
  @GET
  @ApiOperation(value = "Returns all Pipeline labels", response = List.class,
      responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelineLabels() throws PipelineStoreException {
    final List<PipelineInfo> pipelineInfoList = store.getPipelines();
    Set<String> pipelineLabels = new HashSet<>();
    for (PipelineInfo pipelineInfo: pipelineInfoList) {
      Map<String, Object> metadata = pipelineInfo.getMetadata();
      if (metadata != null && metadata.containsKey("labels")) {
        List<String> labels = (List<String>) metadata.get("labels");
        pipelineLabels.addAll(labels);
      }
    }
    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(pipelineLabels)
        .build();
  }

  @Path("/pipelines")
  @GET
  @ApiOperation(value = "Returns all Pipeline Configuration Info", response = PipelineInfoJson.class,
      responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelines(
      @QueryParam("filterText") @DefaultValue("") final String filterText,
      @QueryParam("label") final String label,
      @QueryParam("offset") @DefaultValue("0") int offset,
      @QueryParam("len") @DefaultValue("-1") int len,
      @QueryParam("orderBy") @DefaultValue("NAME") final PipelineOrderByFields orderBy,
      @QueryParam("order") @DefaultValue("ASC") final Order order,
      @QueryParam("includeStatus") @DefaultValue("false") boolean includeStatus
  ) throws PipelineException {
    RestAPIUtils.injectPipelineInMDC("*");

    final List<PipelineInfo> pipelineInfoList = store.getPipelines();
    final Map<String, PipelineState> pipelineStateCache = new HashMap<>();

    Collection<PipelineInfo> filteredCollection = Collections2.filter(pipelineInfoList, new Predicate<PipelineInfo>() {
      @Override
      public boolean apply(PipelineInfo pipelineInfo) {
        String title = pipelineInfo.getTitle() != null ? pipelineInfo.getTitle() : pipelineInfo.getPipelineId();
        if (filterText != null && !title.toLowerCase().contains(filterText.toLowerCase())) {
          return false;
        }
        if (label != null) {
          try {
            Map<String, Object> metadata = pipelineInfo.getMetadata();
            switch (label) {
              case SYSTEM_ALL_PIPELINES:
                return true;
              case SYSTEM_RUNNING_PIPELINES:
                PipelineState state = manager.getPipelineState(pipelineInfo.getPipelineId(), pipelineInfo.getLastRev());
                pipelineStateCache.put(pipelineInfo.getPipelineId(), state);
                return state.getStatus().isActive();
              case SYSTEM_NON_RUNNING_PIPELINES:
                state = manager.getPipelineState(pipelineInfo.getPipelineId(), pipelineInfo.getLastRev());
                pipelineStateCache.put(pipelineInfo.getPipelineId(), state);
                return !state.getStatus().isActive();
              case SYSTEM_INVALID_PIPELINES:
                return !pipelineInfo.isValid();
              case SYSTEM_ERROR_PIPELINES:
                state = manager.getPipelineState(pipelineInfo.getPipelineId(), pipelineInfo.getLastRev());
                pipelineStateCache.put(pipelineInfo.getPipelineId(), state);
                PipelineStatus status = state.getStatus();
                return status == PipelineStatus.START_ERROR ||
                    status == PipelineStatus.RUNNING_ERROR ||
                    status == PipelineStatus.RUN_ERROR ||
                    status == PipelineStatus.CONNECT_ERROR;
              case SYSTEM_PUBLISHED_PIPELINES:
                state = manager.getPipelineState(pipelineInfo.getPipelineId(), pipelineInfo.getLastRev());
                pipelineStateCache.put(pipelineInfo.getPipelineId(), state);
                return !isRemotePipeline(state) && metadata != null && metadata.containsKey(DPM_PIPELINE_ID);
              case SYSTEM_DPM_CONTROLLED_PIPELINES:
                state = manager.getPipelineState(pipelineInfo.getPipelineId(), pipelineInfo.getLastRev());
                pipelineStateCache.put(pipelineInfo.getPipelineId(), state);
                return isRemotePipeline(state);
              case SYSTEM_LOCAL_PIPELINES:
                return metadata == null || !metadata.containsKey(DPM_PIPELINE_ID);
              case SHARED_WITH_ME_PIPELINES:
                return !pipelineInfo.getCreator().equals(user);
              default:
                if (metadata != null && metadata.containsKey("labels")) {
                  List<String> labels = (List<String>) metadata.get("labels");
                  if (!labels.contains(label)) {
                    return false;
                  }
                } else {
                  return false;
                }
            }
          } catch (PipelineException e) {
            e.printStackTrace();
          }
        }
        return true;
      }
    });

    List<PipelineInfo> filteredList = new ArrayList<>(filteredCollection);

    Collections.sort(filteredList, new Comparator<PipelineInfo>() {
      @Override
      public int compare(PipelineInfo p1, PipelineInfo p2) {
        if (order.equals(Order.DESC)) {
          PipelineInfo tmp = p1;
          p1 = p2;
          p2 = tmp;
        }

        if (orderBy.equals(PipelineOrderByFields.NAME)) {
          return p1.getPipelineId().compareTo(p2.getPipelineId());
        }

        if (orderBy.equals(PipelineOrderByFields.TITLE)) {
          String p1Title = p1.getTitle() != null ? p1.getTitle() : p1.getPipelineId();
          String p2Title = p2.getTitle() != null ? p2.getTitle() : p2.getPipelineId();
          return p1Title.compareTo(p2Title);
        }

        if (orderBy.equals(PipelineOrderByFields.LAST_MODIFIED)) {
          return p2.getLastModified().compareTo(p1.getLastModified());
        }

        if (orderBy.equals(PipelineOrderByFields.CREATED)) {
          return p2.getCreated().compareTo(p1.getCreated());
        }

        if (orderBy.equals(PipelineOrderByFields.CREATOR)) {
          return p1.getCreator().compareTo(p2.getCreator());
        }

        if(orderBy.equals(PipelineOrderByFields.STATUS)) {
          try {
            PipelineState p1State = null;
            PipelineState p2State = null;

            if (pipelineStateCache.containsKey(p1.getPipelineId())) {
              p1State = pipelineStateCache.get(p1.getPipelineId());
            } else {
              p1State = manager.getPipelineState(p1.getPipelineId(), p1.getLastRev());
              pipelineStateCache.put(p1.getPipelineId(), p1State);
            }

            if (pipelineStateCache.containsKey(p2.getPipelineId())) {
              p2State = pipelineStateCache.get(p2.getPipelineId());
            } else {
              p2State = manager.getPipelineState(p2.getPipelineId(), p2.getLastRev());
              pipelineStateCache.put(p2.getPipelineId(), p2State);
            }

            if (p1State != null && p2State != null) {
              return p1State.getStatus().compareTo(p2State.getStatus());
            }

          } catch (PipelineException e) {
            LOG.debug("Failed to get Pipeline State - " + e.getLocalizedMessage());
          }
        }

        return 0;
      }
    });

    Object responseData;

    if (filteredList.size() > 0) {
      int endIndex = offset + len;
      if (len == -1 || endIndex > filteredList.size()) {
        endIndex = filteredList.size();
      }
      List<PipelineInfoJson> subList = BeanHelper.wrapPipelineInfo(filteredList.subList(offset, endIndex));
      if (includeStatus) {
        List<PipelineStateJson> statusList = new ArrayList<>(subList.size());
        for (PipelineInfoJson pipelineInfoJson: subList) {
          PipelineState state = pipelineStateCache.get(pipelineInfoJson.getPipelineId());
          if (state == null) {
            state = manager.getPipelineState(pipelineInfoJson.getPipelineId(), pipelineInfoJson.getLastRev());
          }
          if(state != null) {
            statusList.add(BeanHelper.wrapPipelineState(state, true));
          }
        }
        responseData = ImmutableList.of(subList, statusList);
      } else {
        responseData = subList;
      }

    } else {
      if (includeStatus) {
        responseData = ImmutableList.of(Collections.emptyList(), Collections.emptyList());
      } else {
        responseData = Collections.emptyList();
      }
    }

    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(responseData)
        .header("TOTAL_COUNT", filteredList.size())
        .build();
  }

  @Path("/pipelines/delete")
  @POST
  @ApiOperation(value = "Deletes Pipelines", response = PipelineInfoJson.class,
      responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response deletePipelines(
      List<String> pipelineIds,
      @Context SecurityContext context
  ) throws PipelineException {
    RestAPIUtils.injectPipelineInMDC("*");
    for(String pipelineId: pipelineIds) {
      if (store.isRemotePipeline(pipelineId, "0") && !context.isUserInRole(AuthzRole.ADMIN) &&
          !context.isUserInRole(AuthzRole.ADMIN_REMOTE)) {
        throw new PipelineException(ContainerError.CONTAINER_01101, "DELETE_PIPELINE", pipelineId);
      }
      store.deleteRules(pipelineId);
      store.delete(pipelineId);
    }
    return Response.ok().build();
  }

  @Path("/pipelines/deleteByFiltering")
  @POST
  @ApiOperation(value = "Deletes filtered Pipelines", response = PipelineInfoJson.class,
      responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response deletePipelinesByFiltering(
      @QueryParam("filterText") @DefaultValue("") String filterText,
      @QueryParam("label") String label,
      @Context SecurityContext context
  ) throws PipelineException {
    RestAPIUtils.injectPipelineInMDC("*");

    List<PipelineInfo> pipelineInfoList = store.getPipelines();
    List<String> deletepipelineIds = new ArrayList<>();

    for(PipelineInfo pipelineInfo: pipelineInfoList) {
      if (filterText != null && !pipelineInfo.getPipelineId().toLowerCase().contains(filterText.toLowerCase())) {
        continue;
      }

      if (label != null) {
        Map<String, Object> metadata = pipelineInfo.getMetadata();
        if (metadata != null && metadata.containsKey("labels")) {
          List<String> labels = (List<String>) metadata.get("labels");
          if (!labels.contains(label)) {
            continue;
          }
        } else {
          continue;
        }
      }

      if (store.isRemotePipeline(pipelineInfo.getPipelineId(), "0") && !context.isUserInRole(AuthzRole.ADMIN) &&
          !context.isUserInRole(AuthzRole.ADMIN_REMOTE)) {
        continue;
      }
      store.deleteRules(pipelineInfo.getPipelineId());
      store.delete(pipelineInfo.getPipelineId());
      deletepipelineIds.add(pipelineInfo.getPipelineId());
    }

    return Response.ok().entity(deletepipelineIds).build();
  }

  @Path("/pipelines/import")
  @POST
  @ApiOperation(value = "Import Pipelines from compressed archive",
      authorizations = @Authorization(value = "basic"))
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response importPipelines(
      @FormDataParam("file") InputStream uploadedInputStream,
      @Context SecurityContext context
  ) throws PipelineException, IOException {
    RestAPIUtils.injectPipelineInMDC("*");
    List<PipelineInfoJson> successEntities = new ArrayList<>();
    List<String> errorMessages = new ArrayList<>();

    ObjectMapper objectMapper = ObjectMapperFactory.get();
    try (ZipInputStream zipInputStream = new ZipInputStream(uploadedInputStream)) {
      ZipEntry entry;
      while ((entry = zipInputStream.getNextEntry()) != null) {
        if (!entry.getName().startsWith("__MACOSX") && !entry.getName().startsWith(".") &&
            entry.getName().endsWith(".json") && !entry.isDirectory()) {
          try {
            String pipelineEnvelopeString = IOUtils.toString(zipInputStream);
            PipelineEnvelopeJson envelope = objectMapper.readValue(pipelineEnvelopeString, PipelineEnvelopeJson.class);
            importPipelineEnvelope(
                envelope.getPipelineConfig().getTitle(),
                "0",
                false,
                true,
                envelope,
                false
            );
            successEntities.add(envelope.getPipelineConfig().getInfo());
          } catch (Exception ex) {
            errorMessages.add(Utils.format(
                "Failed to import from file: {}. Error: {} ",
                entry.getName(),
                ex.getMessage()
            ));
          }
        }
      }
    }

    return Response.status(207)
        .type(MediaType.APPLICATION_JSON)
        .entity(new MultiStatusResponseJson<>(successEntities, errorMessages)).build();
  }

  @Path("/pipelines/export")
  @POST
  @ApiOperation(value = "Export Pipelines in single compressed archive",
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @PermitAll
  public Response exportPipelines(
      List<String> pipelineIds,
      @QueryParam("includeLibraryDefinitions") @DefaultValue("false") boolean includeLibraryDefinitions,
      @Context SecurityContext context
  ) throws PipelineException {
    RestAPIUtils.injectPipelineInMDC("*");
    String fileName = "pipelines.zip";
    StreamingOutput streamingOutput = output -> {
      ZipOutputStream pipelineZip = new ZipOutputStream(new BufferedOutputStream(output));
      for(String pipelineId: pipelineIds) {
        try {
          PipelineConfiguration pipelineConfig = store.load(pipelineId, "0");
          RuleDefinitions ruleDefinitions = store.retrieveRules(pipelineId, "0");
          PipelineEnvelopeJson pipelineEnvelope = getPipelineEnvelope(
              pipelineConfig,
              ruleDefinitions,
              includeLibraryDefinitions
          );
          pipelineZip.putNextEntry(new ZipEntry(pipelineConfig.getPipelineId() + ".json"));
          pipelineZip.write(ObjectMapperFactory.get().writeValueAsString(pipelineEnvelope).getBytes());
          pipelineZip.closeEntry();
        } catch (Exception ex) {
          LOG.error("Failed to export pipeline: {}", ex.getMessage());
        }
      }
      pipelineZip.close();
    };

    return Response.ok(streamingOutput)
        .header("Content-Disposition", "attachment; filename=\""+ fileName +"\"")
        .build();
  }

  @Path("/pipeline/{pipelineId}")
  @GET
  @ApiOperation(value = "Find Pipeline Configuration by name and revision", response = PipelineConfigurationJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelineInfo(
      @PathParam("pipelineId") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("get") @DefaultValue("pipeline") String get,
      @QueryParam("attachment") @DefaultValue("false") Boolean attachment
  ) throws PipelineException, URISyntaxException {
    PipelineInfo pipelineInfo = store.getInfo(name);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    Object data;
    String title = name;
    if (get.equals("pipeline")) {
      PipelineConfiguration pipeline = store.load(name, rev);
      PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipeline);
      pipeline = validator.validate();
      data = BeanHelper.wrapPipelineConfiguration(pipeline);
      title = pipeline.getTitle() != null ? pipeline.getTitle() : pipeline.getInfo().getPipelineId();
    } else if (get.equals("info")) {
      data = BeanHelper.wrapPipelineInfo(store.getInfo(name));
    } else if (get.equals("history")) {
      data = BeanHelper.wrapPipelineRevInfo(store.getHistory(name));
    } else {
      throw new IllegalArgumentException(Utils.format("Invalid value for parameter 'get': {}", get));
    }

    if (attachment) {
      Map<String, Object> envelope = new HashMap<String, Object>();
      envelope.put("pipelineConfig", data);

      RuleDefinitions ruleDefinitions = store.retrieveRules(name, rev);
      envelope.put("pipelineRules", BeanHelper.wrapRuleDefinitions(ruleDefinitions));

      return Response.ok().
          header("Content-Disposition", "attachment; filename=\"" + title + ".json\"").
          type(MediaType.APPLICATION_JSON).entity(envelope).build();
    } else {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(data).build();
    }
  }

  @Path("/pipeline/{pipelineTitle}")
  @PUT
  @ApiOperation(value = "Add a new Pipeline Configuration to the store", response = PipelineConfigurationJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response createPipeline(
      @PathParam("pipelineTitle") String pipelineTitle,
      @QueryParam("description") @DefaultValue("") String description,
      @QueryParam("autoGeneratePipelineId") @DefaultValue("false") boolean autoGeneratePipelineId,
      @QueryParam("draft") @DefaultValue("false") boolean draft
  ) throws URISyntaxException, PipelineException {
    String pipelineId = pipelineTitle;
    if (autoGeneratePipelineId) {
      pipelineId = pipelineTitle.replaceAll("[\\W]|_", "") + UUID.randomUUID().toString();
    }
    RestAPIUtils.injectPipelineInMDC(pipelineTitle + "/" + pipelineId);
    PipelineConfiguration pipelineConfig = store.create(user, pipelineId, pipelineTitle, description, false, draft);

    //Add predefined Metric Rules to the pipeline
    List<MetricsRuleDefinition> metricsRuleDefinitions = new ArrayList<>();

    long timestamp = System.currentTimeMillis();

    metricsRuleDefinitions.add(new MetricsRuleDefinition(HIGH_BAD_RECORDS_ID, HIGH_BAD_RECORDS_TEXT,
        HIGH_BAD_RECORDS_METRIC_ID, MetricType.COUNTER, MetricElement.COUNTER_COUNT, HIGH_BAD_RECORDS_CONDITION, false,
        false, timestamp));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(HIGH_STAGE_ERRORS_ID, HIGH_STAGE_ERRORS_TEXT,
        HIGH_STAGE_ERRORS_METRIC_ID, MetricType.COUNTER, MetricElement.COUNTER_COUNT, HIGH_STAGE_ERRORS_CONDITION, false,
        false, timestamp));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(PIPELINE_IDLE_ID, PIPELINE_IDLE_TEXT,
        PIPELINE_IDLE_METRIC_ID, MetricType.GAUGE, MetricElement.TIME_OF_LAST_RECEIVED_RECORD, PIPELINE_IDLE_CONDITION,
        false, false, timestamp));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(BATCH_TIME_ID, BATCH_TIME_TEXT, BATCH_TIME_METRIC_ID,
        MetricType.GAUGE, MetricElement.CURRENT_BATCH_AGE, BATCH_TIME_CONDITION, false, false, timestamp));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(MEMORY_LIMIt_ID, MEMORY_LIMIt_TEXT, MEMORY_LIMIt_METRIC_ID,
        MetricType.COUNTER, MetricElement.COUNTER_COUNT, MEMORY_LIMIt_CONDITION, false, false, timestamp));

    RuleDefinitions ruleDefinitions = new RuleDefinitions(
        PipelineStoreTask.RULE_DEFINITIONS_SCHEMA_VERSION,
        RuleDefinitionsConfigBean.VERSION,
        metricsRuleDefinitions,
        Collections.<DataRuleDefinition>emptyList(),
        Collections.<DriftRuleDefinition>emptyList(),
        Collections.<String>emptyList(),
        null,
        stageLibrary.getPipelineRules().getPipelineRulesDefaultConfigs()
    );
    store.storeRules(pipelineId, "0", ruleDefinitions, draft);

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(
        stageLibrary,
        pipelineId,
        pipelineConfig
    );
    pipelineConfig = validator.validate();

    if (draft) {
      return Response.created(UriBuilder.fromUri(uri).path(pipelineId).build())
          .entity(getPipelineEnvelope(pipelineConfig, ruleDefinitions, true))
          .build();
    } else {
      return Response.created(UriBuilder.fromUri(uri).path(pipelineId).build()).entity(
          BeanHelper.wrapPipelineConfiguration(pipelineConfig)).build();
    }
  }

  @Path("/pipeline/{pipelineId}")
  @DELETE
  @ApiOperation(value = "Delete Pipeline Configuration by name", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response deletePipeline(
      @PathParam("pipelineId") String name,
      @Context SecurityContext context
  ) throws URISyntaxException, PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(name);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    if (store.isRemotePipeline(name, "0") && !context.isUserInRole(AuthzRole.ADMIN) &&
        !context.isUserInRole(AuthzRole.ADMIN_REMOTE)) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "DELETE_PIPELINE", name);
    }
    store.deleteRules(name);
    store.delete(name);
    return Response.ok().build();
  }

  @Path("/pipeline/{pipelineId}")
  @POST
  @ApiOperation(value = "Update an existing Pipeline Configuration by name", response = PipelineConfigurationJson.class,
      authorizations = @Authorization(value = "basic"))
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response savePipeline(
      @PathParam("pipelineId") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("description") String description,
      @ApiParam(name="pipeline", required = true) PipelineConfigurationJson pipeline
  ) throws URISyntaxException, PipelineException {
    if (store.isRemotePipeline(name, rev)) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "SAVE_PIPELINE", name);
    }
    PipelineInfo pipelineInfo = store.getInfo(name);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    PipelineConfiguration pipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipeline);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipelineConfig);
    pipelineConfig = validator.validate();
    pipelineConfig = store.save(user, name, rev, description, pipelineConfig);
    return Response.ok().entity(BeanHelper.wrapPipelineConfiguration(pipelineConfig)).build();
  }

  @Path("/pipeline/{pipelineId}/uiInfo")
  @POST
  @ApiOperation(value ="", hidden = true)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  @SuppressWarnings("unchecked")
  public Response saveUiInfo(
      @PathParam("pipelineId") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      Map uiInfo
  ) throws PipelineException, URISyntaxException {
    PipelineInfo pipelineInfo = store.getInfo(name);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    store.saveUiInfo(name, rev, uiInfo);
    return Response.ok().build();
  }

  @Path("/pipeline/{pipelineId}/metadata")
  @POST
  @ApiOperation(value ="", hidden = true)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  @SuppressWarnings("unchecked")
  public Response saveMetadata(
      @PathParam("pipelineId") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      Map<String, Object> metadata
  ) throws PipelineException, URISyntaxException {
    PipelineInfo pipelineInfo = store.getInfo(name);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    store.saveMetadata(user, name, rev, metadata);
    return Response.ok().build();
  }

  @Path("/pipeline/{pipelineId}/rules")
  @GET
  @ApiOperation(value = "Find Pipeline Rules by name and revision", response = RuleDefinitionsJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelineRules(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev
  ) throws PipelineException {
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    RuleDefinitions ruleDefinitions = store.retrieveRules(pipelineId, rev);
    if(ruleDefinitions != null) {
      PipelineConfiguration pipelineConfiguration = store.load(pipelineId, rev);
      PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
          .create(pipelineConfiguration, new ArrayList<>(), null);
      RuleDefinitionValidator ruleDefinitionValidator = new RuleDefinitionValidator(
          pipelineId,
          ruleDefinitions,
          pipelineConfigBean != null ? pipelineConfigBean.constants : null
      );
      ruleDefinitionValidator.validateRuleDefinition();
    }
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
        BeanHelper.wrapRuleDefinitions(ruleDefinitions)).build();
  }

  @Path("/pipeline/{pipelineId}/rules")
  @POST
  @ApiOperation(value = "Update an existing Pipeline Rules by name", response = RuleDefinitionsJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.MANAGER,
      AuthzRole.ADMIN,
      AuthzRole.CREATOR_REMOTE,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response savePipelineRules(
      @PathParam("pipelineId") String pipelineId,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @ApiParam(name="pipeline", required = true) RuleDefinitionsJson ruleDefinitionsJson
  ) throws PipelineException {
    if (store.isRemotePipeline(pipelineId, rev)) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "SAVE_RULES_PIPELINE", pipelineId);
    }
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    RuleDefinitions ruleDefs = BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson);
    PipelineConfiguration pipelineConfiguration = store.load(pipelineId, rev);
    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfiguration, new ArrayList<>(), null);
    RuleDefinitionValidator ruleDefinitionValidator = new RuleDefinitionValidator(
        pipelineId,
        ruleDefs,
        pipelineConfigBean != null ? pipelineConfigBean.constants : null
    );
    ruleDefinitionValidator.validateRuleDefinition();
    ruleDefs = store.storeRules(pipelineId, rev, ruleDefs, false);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapRuleDefinitions(ruleDefs)).build();
  }


  @Path("/pipeline/{pipelineId}/export")
  @GET
  @ApiOperation(value = "Export Pipeline Configuration & Rules by name and revision",
      response = PipelineEnvelopeJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response exportPipeline(
      @PathParam("pipelineId") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("attachment") @DefaultValue("false") Boolean attachment,
      @QueryParam("includeLibraryDefinitions") @DefaultValue("false") boolean includeLibraryDefinitions
  ) throws PipelineException, URISyntaxException {
    PipelineInfo pipelineInfo = store.getInfo(name);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());

    PipelineConfiguration pipelineConfig = store.load(name, rev);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipelineConfig);
    pipelineConfig = validator.validate();
    RuleDefinitions ruleDefinitions = store.retrieveRules(name, rev);
    PipelineEnvelopeJson pipelineEnvelope = getPipelineEnvelope(
        pipelineConfig,
        ruleDefinitions,
        includeLibraryDefinitions
    );

    if (attachment) {
      String fileName = pipelineConfig.getTitle() != null ?
          pipelineConfig.getTitle() : pipelineConfig.getInfo().getPipelineId();
      return Response.ok().
          header("Content-Disposition", "attachment; filename=\"" + fileName + ".json\"").
          type(MediaType.APPLICATION_JSON).entity(pipelineEnvelope).build();
    } else {
      return Response.ok().
          type(MediaType.APPLICATION_JSON).entity(pipelineEnvelope).build();
    }
  }

  private void fetchStageDefinition(
      StageConfiguration conf,
      List<StageDefinition> stageDefinitions,
      Map<String, String> stageIcons
  ) {
    String key = conf.getLibrary() + ":"  + conf.getStageName();
    if (!stageIcons.containsKey(key)) {
      StageDefinition stageDefinition = stageLibrary.getStage(conf.getLibrary(),
          conf.getStageName(), false);
      if (stageDefinition != null) {
        stageDefinitions.add(stageDefinition);
        String iconFile = stageDefinition.getIcon();
        if (iconFile != null && iconFile.trim().length() > 0) {
          try(InputStream icon = stageDefinition.getStageClassLoader().getResourceAsStream(iconFile)) {
            stageIcons.put(key, BaseEncoding.base64().encode(IOUtils.toByteArray(icon)));
          } catch (Exception e) {
            LOG.debug("Failed to convert stage icons to Base64 - " + e.getLocalizedMessage());
            stageIcons.put(key, null);
          }
        } else {
          stageIcons.put(key, null);
        }
      }
    }
  }

  private PipelineEnvelopeJson getPipelineEnvelope(
      PipelineConfiguration pipelineConfig,
      RuleDefinitions ruleDefinitions,
      boolean includeLibraryDefinitions
  ) {
    PipelineEnvelopeJson pipelineEnvelope = new PipelineEnvelopeJson();
    pipelineEnvelope.setPipelineConfig(BeanHelper.wrapPipelineConfiguration(pipelineConfig));
    pipelineEnvelope.setPipelineRules(BeanHelper.wrapRuleDefinitions(ruleDefinitions));
    if (includeLibraryDefinitions) {
      DefinitionsJson definitions = new DefinitionsJson();

      // Add only stage definitions for stages present in pipeline config
      List<StageDefinition> stageDefinitions = new ArrayList<>();
      Map<String, String> stageIcons = new HashMap<>();

      for (StageConfiguration conf : pipelineConfig.getStages()) {
        fetchStageDefinition(conf, stageDefinitions, stageIcons);
      }

      StageConfiguration errorStageConfig = pipelineConfig.getErrorStage();
      if (errorStageConfig != null) {
        fetchStageDefinition(errorStageConfig, stageDefinitions, stageIcons);
      }

      StageConfiguration statsAggregatorStageConfig = pipelineConfig.getStatsAggregatorStage();
      if (statsAggregatorStageConfig != null) {
        fetchStageDefinition(statsAggregatorStageConfig, stageDefinitions, stageIcons);
      }

      for (StageConfiguration startEventStage: pipelineConfig.getStartEventStages()) {
        fetchStageDefinition(startEventStage, stageDefinitions, stageIcons);
      }

      for (StageConfiguration stopEventStage: pipelineConfig.getStopEventStages()) {
        fetchStageDefinition(stopEventStage, stageDefinitions, stageIcons);
      }

      List<StageDefinitionJson> stages = new ArrayList<>();
      stages.addAll(BeanHelper.wrapStageDefinitions(stageDefinitions));
      definitions.setStages(stages);

      definitions.setStageIcons(stageIcons);

      List<PipelineDefinitionJson> pipeline = new ArrayList<>(1);
      pipeline.add(BeanHelper.wrapPipelineDefinition(stageLibrary.getPipeline()));
      definitions.setPipeline(pipeline);

      List<PipelineRulesDefinitionJson> pipelineRules = new ArrayList<>(1);
      pipelineRules.add(BeanHelper.wrapPipelineRulesDefinition(stageLibrary.getPipelineRules()));
      definitions.setPipelineRules(pipelineRules);

      pipelineEnvelope.setLibraryDefinitions(definitions);
    }

    return pipelineEnvelope;
  }

  @Path("/pipeline/{pipelineId}/import")
  @POST
  @ApiOperation(value = "Import Pipeline Configuration & Rules", response = PipelineEnvelopeJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response importPipeline(
      @PathParam("pipelineId") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("overwrite") @DefaultValue("false") boolean overwrite,
      @QueryParam("autoGeneratePipelineId") @DefaultValue("false") boolean autoGeneratePipelineId,
      @QueryParam("draft") @DefaultValue("false") boolean draft,
      @ApiParam(name="pipelineEnvelope", required = true) PipelineEnvelopeJson pipelineEnvelope
  ) throws PipelineException, URISyntaxException {
    RestAPIUtils.injectPipelineInMDC("*");
    pipelineEnvelope = importPipelineEnvelope(name, rev, overwrite, autoGeneratePipelineId, pipelineEnvelope, draft);
    return Response.ok().
        type(MediaType.APPLICATION_JSON).entity(pipelineEnvelope).build();
  }

  private PipelineEnvelopeJson importPipelineEnvelope(
      String name,
      String rev,
      boolean overwrite,
      boolean autoGeneratePipelineId,
      PipelineEnvelopeJson pipelineEnvelope,
      boolean draft
  ) throws PipelineException {
    PipelineConfigurationJson pipelineConfigurationJson = pipelineEnvelope.getPipelineConfig();
    PipelineConfiguration pipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipelineConfigurationJson);

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipelineConfig);
    pipelineConfig = validator.validate();

    RuleDefinitionsJson ruleDefinitionsJson = pipelineEnvelope.getPipelineRules();
    RuleDefinitions ruleDefinitions = BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson);

    PipelineConfiguration newPipelineConfig;
    RuleDefinitions newRuleDefinitions;

    String label = name;

    if (overwrite) {
      if (store.hasPipeline(name)) {
        newPipelineConfig = store.load(name, rev);
      } else {
        if (autoGeneratePipelineId) {
          name = UUID.randomUUID().toString();
        }
        newPipelineConfig = store.create(user, name, label, pipelineConfig.getDescription(), false, draft);
      }
    } else {
      if (autoGeneratePipelineId) {
        name = UUID.randomUUID().toString();
      }
      newPipelineConfig = store.create(user, name, label, pipelineConfig.getDescription(), false, draft);
    }

    if (!draft) {
      newRuleDefinitions = store.retrieveRules(name, rev);
      ruleDefinitions.setUuid(newRuleDefinitions.getUuid());

      pipelineConfig.setUuid(newPipelineConfig.getUuid());
      pipelineConfig.setPipelineId(newPipelineConfig.getPipelineId());
      pipelineConfig = store.save(user, name, rev, pipelineConfig.getDescription(), pipelineConfig);
    }

    PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
        .create(pipelineConfig, Collections.emptyList(), null);
    RuleDefinitionValidator ruleDefinitionValidator = new RuleDefinitionValidator(
        name,
        ruleDefinitions,
        pipelineConfigBean.constants
    );
    ruleDefinitionValidator.validateRuleDefinition();

    if ((!draft)) {
      ruleDefinitions = store.storeRules(name, rev, ruleDefinitions, false);
    }

    return getPipelineEnvelope(pipelineConfig, ruleDefinitions, draft);
  }

  @Path("/pipelines/addLabels")
  @POST
  @ApiOperation(value = "Add labels to multiple Pipelines", response = MultiStatusResponseJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR,
      AuthzRole.ADMIN,
      AuthzRole.MANAGER_REMOTE,
      AuthzRole.ADMIN_REMOTE
  })
  public Response addLabelsToPipelines(AddLabelsRequestJson addLabelsRequestJson) throws PipelineException {
    List<String> labels = addLabelsRequestJson.getLabels();
    List<String> pipelineIds = addLabelsRequestJson.getPipelineNames();
    List<String> successEntities = new ArrayList<>();
    List<String> errorMessages = new ArrayList<>();

    for (String pipelineId: pipelineIds) {
      try {
        PipelineConfiguration pipelineConfig = store.load(pipelineId, "0");
        Map<String, Object> metadata = pipelineConfig.getMetadata();

        Object objLabels = metadata.get("labels");
        List<String> metaLabels = objLabels == null ? new ArrayList<>() : (List<String>) objLabels;

        for (String label : labels) {
          if (!metaLabels.contains(label)) {
            metaLabels.add(label);
          }
        }

        metadata.put("labels", metaLabels);
        RestAPIUtils.injectPipelineInMDC(pipelineConfig.getInfo().getTitle(), pipelineId);
        PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, pipelineId,
            pipelineConfig);
        pipelineConfig = validator.validate();
        store.save(user, pipelineId, "0", pipelineConfig.getDescription(), pipelineConfig);
        successEntities.add(pipelineId);

      } catch (Exception ex) {
        errorMessages.add("Failed adding labels " + labels + " to pipeline: " + pipelineId + ". Error: " +
            ex.getMessage());
      }
    }

    return Response.status(207)
        .type(MediaType.APPLICATION_JSON)
        .entity(new MultiStatusResponseJson<>(successEntities, errorMessages)).build();
  }

  private boolean isRemotePipeline(PipelineState state) {
    Object isRemote = state.getAttributes().get(RemoteDataCollector.IS_REMOTE_PIPELINE);
    return isRemote != null && (boolean) isRemote;
  }

  @GET
  @Path("/pipelines/executable")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  @ApiOperation(
      value = "Generate Edge Executable",
      authorizations = @Authorization(value = "basic")
  )
  public Response getEdgeExecutable(
      @QueryParam("edgeOs") @DefaultValue("darwin") String edgeOs,
      @QueryParam("edgeArch") @DefaultValue("amd64") String edgeArch,
      @QueryParam("pipelineIds") String pipelineIds
  ) throws IOException, PipelineException {

    String[] pipelineIdArr = pipelineIds.split(",");
    List<PipelineConfigurationJson> pipelineConfigurationList = new ArrayList<>();

    for (String pipelineId: pipelineIdArr) {
      PipelineConfiguration pipelineConfiguration = store.load(pipelineId, "0");
      PipelineConfigBean pipelineConfigBean =  PipelineBeanCreator.get()
          .create(pipelineConfiguration, new ArrayList<>(), null);
      if (!pipelineConfigBean.executionMode.equals(ExecutionMode.EDGE)) {
        throw new PipelineException(ContainerError.CONTAINER_01600, pipelineConfigBean.executionMode);
      }
      pipelineConfigurationList.add(BeanHelper.wrapPipelineConfiguration(pipelineConfiguration));
    }

    EdgeExecutableStreamingOutput streamingOutput = new EdgeExecutableStreamingOutput(
        runtimeInfo,
        edgeOs,
        edgeArch,
        pipelineConfigurationList
    );

    return Response.ok(streamingOutput)
        .header("Content-Disposition", "attachment; filename=\""+ streamingOutput.getFileName() +"\"")
        .build();
  }
}
