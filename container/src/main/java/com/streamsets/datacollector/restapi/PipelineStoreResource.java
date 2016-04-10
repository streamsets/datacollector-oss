/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.restapi;

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
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.DefinitionsJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineDefinitionJson;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.PipelineInfoJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.restapi.bean.StageDefinitionJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.datacollector.validation.RuleDefinitionValidator;
import com.streamsets.pipeline.api.impl.Utils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;

import org.apache.commons.io.IOUtils;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v1")
@Api(value = "store")
@DenyAll
public class PipelineStoreResource {
  private static final String HIGH_BAD_RECORDS_ID = "badRecordsAlertID";
  private static final String HIGH_BAD_RECORDS_TEXT = "High incidence of Bad Records";
  private static final String HIGH_BAD_RECORDS_METRIC_ID = "pipeline.batchErrorRecords.meter";
  private static final String HIGH_BAD_RECORDS_CONDITION = "${value() > 100}";

  private static final String HIGH_STAGE_ERRORS_ID = "stageErrorAlertID";
  private static final String HIGH_STAGE_ERRORS_TEXT = "High incidence of Error Messages";
  private static final String HIGH_STAGE_ERRORS_METRIC_ID = "pipeline.batchErrorMessages.meter";
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

  private static final Logger LOG = LoggerFactory.getLogger(PipelineStoreResource.class);


  private final RuntimeInfo runtimeInfo;
  private final PipelineStoreTask store;
  private final StageLibraryTask stageLibrary;
  private final URI uri;
  private final String user;

  @Inject
  public PipelineStoreResource(URI uri, Principal user, StageLibraryTask stageLibrary, PipelineStoreTask store,
                               RuntimeInfo runtimeInfo) {
    this.uri = uri;
    this.user = user.getName();
    this.stageLibrary = stageLibrary;
    this.store = store;
    this.runtimeInfo = runtimeInfo;
  }

  @Path("/pipelines")
  @GET
  @ApiOperation(value = "Returns all Pipeline Configuration Info", response = PipelineInfoJson.class,
    responseContainer = "List", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelines() throws PipelineStoreException {
    RestAPIUtils.injectPipelineInMDC("*");
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapPipelineInfo(store.getPipelines()))
      .build();
  }

  @Path("/pipeline/{pipelineName}")
  @GET
  @ApiOperation(value = "Find Pipeline Configuration by name and revision", response = PipelineConfigurationJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelineInfo(
      @PathParam("pipelineName") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("get") @DefaultValue("pipeline") String get,
      @QueryParam("attachment") @DefaultValue("false") Boolean attachment)
      throws PipelineStoreException, URISyntaxException {
    RestAPIUtils.injectPipelineInMDC(name);
    Object data;
    if (get.equals("pipeline")) {
      PipelineConfiguration pipeline = store.load(name, rev);
      PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipeline);
      pipeline = validator.validate();
      data = BeanHelper.wrapPipelineConfiguration(pipeline);
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

      com.streamsets.datacollector.config.RuleDefinitions ruleDefinitions = store.retrieveRules(name, rev);
      envelope.put("pipelineRules", BeanHelper.wrapRuleDefinitions(ruleDefinitions));

      return Response.ok().
        header("Content-Disposition", "attachment; filename=" + name + ".json").
        type(MediaType.APPLICATION_JSON).entity(envelope).build();
    } else
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(data).build();

  }

  @Path("/pipeline/{pipelineName}")
  @PUT
  @ApiOperation(value = "Add a new Pipeline Configuration to the store", response = PipelineConfigurationJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response createPipeline(
      @PathParam("pipelineName") String name,
      @QueryParam("description") @DefaultValue("") String description)
      throws URISyntaxException, PipelineException {
    RestAPIUtils.injectPipelineInMDC(name);
    PipelineConfiguration pipeline = store.create(user, name, description, false);

    //Add predefined Metric Rules to the pipeline
    List<MetricsRuleDefinition> metricsRuleDefinitions = new ArrayList<>();

    long timestamp = System.currentTimeMillis();

    metricsRuleDefinitions.add(new MetricsRuleDefinition(HIGH_BAD_RECORDS_ID, HIGH_BAD_RECORDS_TEXT,
      HIGH_BAD_RECORDS_METRIC_ID, MetricType.METER, MetricElement.METER_COUNT, HIGH_BAD_RECORDS_CONDITION, false,
      false, timestamp));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(HIGH_STAGE_ERRORS_ID, HIGH_STAGE_ERRORS_TEXT,
      HIGH_STAGE_ERRORS_METRIC_ID, MetricType.METER, MetricElement.METER_COUNT, HIGH_STAGE_ERRORS_CONDITION, false,
      false, timestamp));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(PIPELINE_IDLE_ID, PIPELINE_IDLE_TEXT,
      PIPELINE_IDLE_METRIC_ID, MetricType.GAUGE, MetricElement.TIME_OF_LAST_RECEIVED_RECORD, PIPELINE_IDLE_CONDITION,
      false, false, timestamp));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(BATCH_TIME_ID, BATCH_TIME_TEXT, BATCH_TIME_METRIC_ID,
      MetricType.GAUGE, MetricElement.CURRENT_BATCH_AGE, BATCH_TIME_CONDITION, false, false, timestamp));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(MEMORY_LIMIt_ID, MEMORY_LIMIt_TEXT, MEMORY_LIMIt_METRIC_ID,
      MetricType.COUNTER, MetricElement.COUNTER_COUNT, MEMORY_LIMIt_CONDITION, false, false, timestamp));

    RuleDefinitions ruleDefinitions = new RuleDefinitions(
        metricsRuleDefinitions,
        Collections.<DataRuleDefinition>emptyList(),
        Collections.<DriftRuleDefinition>emptyList(),
        Collections.<String>emptyList(),
        null
    );
    store.storeRules(name, "0", ruleDefinitions);

    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipeline);
    pipeline = validator.validate();
    return Response.created(UriBuilder.fromUri(uri).path(name).build()).entity(
      BeanHelper.wrapPipelineConfiguration(pipeline)).build();
  }

  @Path("/pipeline/{pipelineName}")
  @DELETE
  @ApiOperation(value = "Delete Pipeline Configuration by name", authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response deletePipeline(
      @PathParam("pipelineName") String name)
      throws URISyntaxException, PipelineException {
    RestAPIUtils.injectPipelineInMDC(name);
    if (store.isRemotePipeline(name, "0")) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "DELETE_PIPELINE", name);
    }
    store.delete(name);
    store.deleteRules(name);
    return Response.ok().build();
  }

  @Path("/pipeline/{pipelineName}")
  @POST
  @ApiOperation(value = "Update an existing Pipeline Configuration by name", response = PipelineConfigurationJson.class,
    authorizations = @Authorization(value = "basic"))
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  public Response savePipeline(
      @PathParam("pipelineName") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("description") String description,
      @ApiParam(name="pipeline", required = true) PipelineConfigurationJson pipeline)
      throws URISyntaxException, PipelineException {
    if (store.isRemotePipeline(name, rev)) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "SAVE_PIPELINE", name);
    }
    RestAPIUtils.injectPipelineInMDC(name);
    PipelineConfiguration pipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipeline);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipelineConfig);
    pipelineConfig = validator.validate();
    pipelineConfig = store.save(user, name, rev, description, pipelineConfig);
    return Response.ok().entity(BeanHelper.wrapPipelineConfiguration(pipelineConfig)).build();
  }

  @Path("/pipeline/{pipelineName}/uiInfo")
  @POST
  @ApiOperation(value ="", hidden = true)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed({
      AuthzRole.CREATOR, AuthzRole.ADMIN, AuthzRole.CREATOR_REMOTE, AuthzRole.ADMIN_REMOTE
  })
  @SuppressWarnings("unchecked")
  public Response saveUiInfo(
      @PathParam("pipelineName") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      Map uiInfo)
      throws PipelineStoreException, URISyntaxException {
    store.saveUiInfo(name, rev, uiInfo);
    return Response.ok().build();
  }

  @Path("/pipeline/{pipelineName}/rules")
  @GET
  @ApiOperation(value = "Find Pipeline Rules by name and revision", response = RuleDefinitionsJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getPipelineRules(
    @PathParam("pipelineName") String name,
    @QueryParam("rev") @DefaultValue("0") String rev) throws PipelineStoreException {
    RestAPIUtils.injectPipelineInMDC(name);
    RuleDefinitions ruleDefinitions = store.retrieveRules(name, rev);
    if(ruleDefinitions != null) {
      RuleDefinitionValidator ruleDefinitionValidator = new RuleDefinitionValidator();
      ruleDefinitionValidator.validateRuleDefinition(ruleDefinitions);
    }
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(
      BeanHelper.wrapRuleDefinitions(ruleDefinitions)).build();
  }

  @Path("/pipeline/{pipelineName}/rules")
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
    @PathParam("pipelineName") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @ApiParam(name="pipeline", required = true) RuleDefinitionsJson ruleDefinitionsJson) throws PipelineException {
    if (store.isRemotePipeline(name, rev)) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "SAVE_RULES_PIPELINE", name);
    }
    RestAPIUtils.injectPipelineInMDC(name);
    RuleDefinitions ruleDefs = BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson);
    RuleDefinitionValidator ruleDefinitionValidator = new RuleDefinitionValidator();
    ruleDefinitionValidator.validateRuleDefinition(ruleDefs);
    ruleDefs = store.storeRules(name, rev, ruleDefs);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapRuleDefinitions(ruleDefs)).build();
  }


  @Path("/pipeline/{pipelineName}/export")
  @GET
  @ApiOperation(value = "Export Pipeline Configuration & Rules by name and revision",
      response = PipelineEnvelopeJson.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response exportPipeline(
      @PathParam("pipelineName") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("attachment") @DefaultValue("false") Boolean attachment,
      @QueryParam("includeLibraryDefinitions") @DefaultValue("false") boolean includeLibraryDefinitions
  ) throws PipelineStoreException, URISyntaxException {
    RestAPIUtils.injectPipelineInMDC(name);
    PipelineConfiguration pipelineConfig = store.load(name, rev);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipelineConfig);
    pipelineConfig = validator.validate();

    RuleDefinitions ruleDefinitions = store.retrieveRules(name, rev);

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

      List<StageDefinitionJson> stages = new ArrayList<>();
      stages.addAll(BeanHelper.wrapStageDefinitions(stageDefinitions));
      definitions.setStages(stages);

      definitions.setStageIcons(stageIcons);

      List<PipelineDefinitionJson> pipeline = new ArrayList<>(1);
      pipeline.add(BeanHelper.wrapPipelineDefinition(stageLibrary.getPipeline()));
      definitions.setPipeline(pipeline);

      pipelineEnvelope.setLibraryDefinitions(definitions);
    }

    if (attachment) {
      return Response.ok().
          header("Content-Disposition", "attachment; filename=" + name + ".json").
          type(MediaType.APPLICATION_JSON).entity(pipelineEnvelope).build();
    } else {
      return Response.ok().
          type(MediaType.APPLICATION_JSON).entity(pipelineEnvelope).build();
    }
  }

  private void fetchStageDefinition(StageConfiguration conf,
                                    List<StageDefinition> stageDefinitions,
                                    Map<String, String> stageIcons) {
    String key = conf.getLibrary() + ":"  + conf.getStageName();
    if (!stageIcons.containsKey(key)) {
      StageDefinition stageDefinition = stageLibrary.getStage(conf.getLibrary(),
          conf.getStageName(), false);
      if (stageDefinition != null) {
        stageDefinitions.add(stageDefinition);
        String iconFile = stageDefinition.getIcon();
        if (iconFile != null && iconFile.trim().length() > 0) {
          try {
            stageIcons.put(key, BaseEncoding.base64().encode(
                IOUtils.toByteArray(stageDefinition.getStageClassLoader().getResourceAsStream(iconFile))));
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

  @Path("/pipeline/{pipelineName}/import")
  @POST
  @ApiOperation(value = "Import Pipeline Configuration & Rules", response = PipelineEnvelopeJson.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response importPipeline(
      @PathParam("pipelineName") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("overwrite") @DefaultValue("false") boolean overwrite,
      @ApiParam(name="pipelineEnvelope", required = true) PipelineEnvelopeJson pipelineEnvelope
  ) throws PipelineStoreException, URISyntaxException {
    RestAPIUtils.injectPipelineInMDC(name);

    PipelineConfigurationJson pipelineConfigurationJson = pipelineEnvelope.getPipelineConfig();
    PipelineConfiguration pipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipelineConfigurationJson);

    RuleDefinitionsJson ruleDefinitionsJson = pipelineEnvelope.getPipelineRules();
    RuleDefinitions ruleDefinitions = BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson);

    PipelineConfiguration newPipelineConfig;
    RuleDefinitions newRuleDefinitions;

    if (overwrite) {
      if (store.hasPipeline(name)) {
        newPipelineConfig = store.load(name, rev);
      } else {
        newPipelineConfig = store.create(user, name, pipelineConfig.getDescription(), false);
      }
    } else {
      newPipelineConfig = store.create(user, name, pipelineConfig.getDescription(), false);
    }

    newRuleDefinitions = store.retrieveRules(name, rev);

    pipelineConfig.setUuid(newPipelineConfig.getUuid());
    pipelineConfig = store.save(user, name, rev, pipelineConfig.getDescription(), pipelineConfig);

    ruleDefinitions.setUuid(newRuleDefinitions.getUuid());
    ruleDefinitions = store.storeRules(name, rev, ruleDefinitions);

    pipelineEnvelope.setPipelineConfig(BeanHelper.wrapPipelineConfiguration(pipelineConfig));
    pipelineEnvelope.setPipelineRules(BeanHelper.wrapRuleDefinitions(ruleDefinitions));
    return Response.ok().
        type(MediaType.APPLICATION_JSON).entity(pipelineEnvelope).build();
  }
}
