/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.config.DataRuleDefinition;
import com.streamsets.datacollector.config.MetricElement;
import com.streamsets.datacollector.config.MetricType;
import com.streamsets.datacollector.config.MetricsRuleDefinition;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.RuleDefinitions;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineInfoJson;
import com.streamsets.datacollector.restapi.bean.RuleDefinitionsJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.datacollector.validation.RuleDefinitionValidator;
import com.streamsets.pipeline.api.impl.Utils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;

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

    if(attachment) {
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
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response createPipeline(
      @PathParam("pipelineName") String name,
      @QueryParam("description") @DefaultValue("") String description)
      throws PipelineStoreException, URISyntaxException {
    RestAPIUtils.injectPipelineInMDC(name);
    PipelineConfiguration pipeline = store.create(user, name, description);

    //Add predefined Metric Rules to the pipeline
    List<MetricsRuleDefinition> metricsRuleDefinitions = new ArrayList<>();

    metricsRuleDefinitions.add(new MetricsRuleDefinition(HIGH_BAD_RECORDS_ID, HIGH_BAD_RECORDS_TEXT,
      HIGH_BAD_RECORDS_METRIC_ID, MetricType.METER, MetricElement.METER_COUNT, HIGH_BAD_RECORDS_CONDITION, false,
      false));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(HIGH_STAGE_ERRORS_ID, HIGH_STAGE_ERRORS_TEXT,
      HIGH_STAGE_ERRORS_METRIC_ID, MetricType.METER, MetricElement.METER_COUNT, HIGH_STAGE_ERRORS_CONDITION, false,
      false));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(PIPELINE_IDLE_ID, PIPELINE_IDLE_TEXT,
      PIPELINE_IDLE_METRIC_ID, MetricType.GAUGE, MetricElement.TIME_OF_LAST_RECEIVED_RECORD, PIPELINE_IDLE_CONDITION,
      false, false));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(BATCH_TIME_ID, BATCH_TIME_TEXT, BATCH_TIME_METRIC_ID,
      MetricType.GAUGE, MetricElement.CURRENT_BATCH_AGE, BATCH_TIME_CONDITION, false, false));

    metricsRuleDefinitions.add(new MetricsRuleDefinition(MEMORY_LIMIt_ID, MEMORY_LIMIt_TEXT, MEMORY_LIMIt_METRIC_ID,
      MetricType.COUNTER, MetricElement.COUNTER_COUNT, MEMORY_LIMIt_CONDITION, false, false));

    RuleDefinitions ruleDefinitions = new RuleDefinitions(metricsRuleDefinitions,
      Collections.<DataRuleDefinition>emptyList(), Collections.<String>emptyList(), null);
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
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response deletePipeline(
      @PathParam("pipelineName") String name)
      throws PipelineStoreException, URISyntaxException {
    RestAPIUtils.injectPipelineInMDC(name);
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
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
  public Response savePipeline(
      @PathParam("pipelineName") String name,
      @QueryParam("rev") @DefaultValue("0") String rev,
      @QueryParam("description") String description,
      @ApiParam(name="pipeline", required = true) PipelineConfigurationJson pipeline)
      throws PipelineStoreException, URISyntaxException {
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
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.ADMIN })
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
  @RolesAllowed({ AuthzRole.CREATOR, AuthzRole.MANAGER, AuthzRole.ADMIN })
  public Response savePipelineRules(
    @PathParam("pipelineName") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @ApiParam(name="pipeline", required = true) RuleDefinitionsJson ruleDefinitionsJson) throws PipelineStoreException {
    RestAPIUtils.injectPipelineInMDC(name);
    RuleDefinitions ruleDefs = BeanHelper.unwrapRuleDefinitions(ruleDefinitionsJson);
    RuleDefinitionValidator ruleDefinitionValidator = new RuleDefinitionValidator();
    ruleDefinitionValidator.validateRuleDefinition(ruleDefs);
    ruleDefs = store.storeRules(name, rev, ruleDefs);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(BeanHelper.wrapRuleDefinitions(ruleDefs)).build();
  }

}
