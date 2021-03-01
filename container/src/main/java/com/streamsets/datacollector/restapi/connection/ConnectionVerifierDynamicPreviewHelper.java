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

package com.streamsets.datacollector.restapi.connection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.ConnectionConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.json.PipelineConfigAndRulesJson;
import com.streamsets.datacollector.dynamicpreview.DynamicPreviewRequestJson;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.json.DynamicPreviewEventJson;
import com.streamsets.datacollector.event.json.PipelinePreviewEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.PipelineStopAndDeleteEventJson;
import com.streamsets.datacollector.execution.preview.common.PreviewError;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.ConfigConfigurationJson;
import com.streamsets.datacollector.restapi.bean.ConnectionDefinitionPreviewJson;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.StageConfigurationJson;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.StageException;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ConnectionVerifierDynamicPreviewHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionVerifierDynamicPreviewHelper.class);

  private StageLibraryTask stageLibraryTask;

  public ConnectionVerifierDynamicPreviewHelper(StageLibraryTask stageLibraryTask) {
    this.stageLibraryTask = stageLibraryTask;
  }

  /**
   * Build a VERIFIER >> TRASH pipeline for the given connection, based on the pipeline template
   *
   * To build the pipeline we load a pipeline json file that will be used as a template, and replace the
   * existing RAW_DATA origin with a newly constructed connection verifier source
   *
   * @param connection The connection to preview
   * @return The VERIFIER >> TRASH pipeline
   * @throws IOException if the pipeline template cannot be loaded
   */
  public PipelineEnvelopeJson getVerifierDynamicPreviewPipeline(ConnectionDefinitionPreviewJson connection) throws IOException {
    // get pipeline template
    String pipelineTemplateRawJson = getTemplatePipelineDefinitionJson();
    PipelineEnvelopeJson pipelineTemplate =
        ObjectMapperFactory.get().readValue(pipelineTemplateRawJson, PipelineEnvelopeJson.class);
    PipelineConfigurationJson pipelineTemplateConfig = pipelineTemplate.getPipelineConfig();

    // build verifier stage and replace the raw data origin with it
    StageDefinition verifierStageDef = stageLibraryTask.getStage(connection.getVerifierDefinition().getLibrary(),
        connection.getVerifierStageName(), false);
    if (verifierStageDef == null) {
      throw new StageException(
          PreviewError.PREVIEW_0106,
          connection.getVerifierStageName(),
          connection.getVerifierDefinition().getLibrary());
    }
    List<ConfigConfigurationJson> verifierConfig = new ArrayList<>();
    verifierConfig.add(new ConfigConfigurationJson(
        connection.getVerifierDefinition().getVerifierConnectionSelectionFieldName(),
        connection.getConnectionId())
    );
    StageConfigurationJson rawDataStage = pipelineTemplateConfig.getStages().get(0);
    rawDataStage.getUiInfo().put("label", "Connection Verifier");
    StageConfigurationJson verifierStage = new StageConfigurationJson(
        connection.getVerifierStageName().concat("_01"),
        connection.getVerifierDefinition().getLibrary(),
        connection.getVerifierStageName(),
        String.valueOf(verifierStageDef.getVersion()),
        verifierConfig,
        rawDataStage.getUiInfo(),
        ImmutableList.of(),
        rawDataStage.getOutputLanes(),
        ImmutableList.of(),
        ImmutableList.of()
    );

    List<StageConfigurationJson> verifierStages = Arrays.asList(verifierStage, pipelineTemplateConfig.getStages().get(1));
    String pipelineId = UUID.randomUUID().toString() + "-Dynamic-Preview";
    PipelineConfigurationJson verifierConfiguration = new PipelineConfigurationJson(
        1,
        Integer.valueOf(connection.getVersion()),
        pipelineId,
        "Dynamic Preview Pipeline",
        "Temporary Pipeline for the Connection Verifier Dynamic Preview",
        UUID.randomUUID(),
        pipelineTemplateConfig.getConfiguration(),
        pipelineTemplateConfig.getUiInfo(),
        pipelineTemplateConfig.getFragments(),
        verifierStages,
        pipelineTemplateConfig.getErrorStage(),
        pipelineTemplateConfig.getInfo(),
        pipelineTemplateConfig.getMetadata(),
        pipelineTemplateConfig.getStatsAggregatorStage(),
        pipelineTemplateConfig.getStartEventStages(),
        pipelineTemplateConfig.getStopEventStages(),
        pipelineTemplateConfig.getTestOriginStage()
    );

    // build new pipeline with same rules and updated configuration
    PipelineEnvelopeJson verifierPipeline = new PipelineEnvelopeJson();
    verifierPipeline.setPipelineConfig(verifierConfiguration);
    verifierPipeline.setPipelineRules(pipelineTemplate.getPipelineRules());

    return verifierPipeline;
  }

  /**
   * Builds the DynamicPreviewEvent for the given verifier pipeline. The Dynamic Preview will have 3 events:
   * - Preview event to run the preview
   * - Before action to save the temporary verifier pipeline
   * - After action to delete the temporary verifier pipeline
   *
   * @param verifierPipeline the pipeline on which the preview will be executed
   * @param dynamicPreviewRequest the request with the preview configuration
   * @param currentUser the user making the dynamic preview request
   * @return The DynamicPreviewEvent
   */
  public DynamicPreviewEventJson getVerifierDynamicPreviewEvent(
      PipelineEnvelopeJson verifierPipeline,
      DynamicPreviewRequestJson dynamicPreviewRequest,
      UserJson currentUser
  ) {
    // build preview, save & delete pipeline events
    PipelinePreviewEventJson pipelinePreviewEvent = getPipelinePreviewEvent(
        verifierPipeline.getPipelineConfig().getPipelineId(),
        dynamicPreviewRequest,
        currentUser
    );
    PipelineSaveEventJson pipelineSaveEvent = getPipelineSaveEvent(verifierPipeline, currentUser);
    PipelineStopAndDeleteEventJson pipelineDeleteEvent = getPipelineDeleteEvent(pipelineSaveEvent);

    // build dynamic preview event
    DynamicPreviewEventJson.Builder previewBuilder = new DynamicPreviewEventJson.Builder();
    previewBuilder.setPreviewEvent(pipelinePreviewEvent, EventType.PREVIEW_PIPELINE.getValue());
    previewBuilder.addBeforeAction(pipelineSaveEvent, EventType.SAVE_PIPELINE.getValue());
    previewBuilder.addAfterAction(pipelineDeleteEvent, EventType.STOP_DELETE_PIPELINE.getValue());
    return previewBuilder.build();
  }

  /**
   * Generates a PipelinePreviewEvent for the pipeline with the given id
   *
   * @param pipelineId The pipeline id
   * @param dynamicPreviewRequest the request with the preview configuration parameters
   * @param currentUser the user making the dynamic preview request
   * @return The PipelinePreviewEvent
   */
  private PipelinePreviewEventJson getPipelinePreviewEvent(
          String pipelineId,
          DynamicPreviewRequestJson dynamicPreviewRequest,
          UserJson currentUser
  ) {
    final PipelinePreviewEventJson previewEvent = new PipelinePreviewEventJson();
    previewEvent.setName(pipelineId);
    previewEvent.setUser(currentUser.getName());
    previewEvent.setRev("0");
    previewEvent.setBatches(dynamicPreviewRequest.getBatches());
    previewEvent.setBatchSize(dynamicPreviewRequest.getBatchSize());
    previewEvent.setTestOrigin(dynamicPreviewRequest.isTestOrigin());
    previewEvent.setTimeoutMillis(dynamicPreviewRequest.getTimeout());
    previewEvent.setSkipTargets(dynamicPreviewRequest.isSkipTargets());
    previewEvent.setSkipLifecycleEvents(dynamicPreviewRequest.isSkipLifecycleEvents());
    previewEvent.setGroups(currentUser.getGroups());
    return previewEvent;
  }

  /**
   * Generates the PipelineSaveEvent for the given pipeline, to be used as before action in the preview events
   *
   * @param pipelineEnvelopeJson The pipeline to save
   * @param currentUser the user making the dynamic preview request
   * @return The PipelineSaveEvent for the given pipeline
   */
  private PipelineSaveEventJson getPipelineSaveEvent(PipelineEnvelopeJson pipelineEnvelopeJson, UserJson currentUser) {
    final PipelineSaveEventJson pipelineSaveEventJson = new PipelineSaveEventJson();
    final PipelineConfigAndRulesJson configAndRules = new PipelineConfigAndRulesJson();
    try {
      final ObjectMapper objMapper = ObjectMapperFactory.get();
      configAndRules.setPipelineConfig(objMapper.writeValueAsString(pipelineEnvelopeJson.getPipelineConfig()));
      configAndRules.setPipelineRules(objMapper.writeValueAsString(pipelineEnvelopeJson.getPipelineRules()));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("JsonProcessingException attempting to write pipeline config or rules as String", e);
    }
    pipelineSaveEventJson.setPipelineConfigurationAndRules(configAndRules);
    pipelineSaveEventJson.setName(pipelineEnvelopeJson.getPipelineConfig().getPipelineId());
    pipelineSaveEventJson.setDescription("Dynamic Preview Pipeline for Connection Verifier");
    pipelineSaveEventJson.setUser(currentUser.getName());
    pipelineSaveEventJson.setRev("0");
    return pipelineSaveEventJson;
  }

  /**
   * Generates a PipelineStopAndDeleteEventJson for the pipeline created by the given PipelineSaveEventJson
   *
   * @param saveEvent The PipelineSaveEventJson that created the pipeline that we want to delete
   * @return The PipelineStopAndDeleteEventJson to stop and delete the pipeline
   */
  private static PipelineStopAndDeleteEventJson getPipelineDeleteEvent(PipelineSaveEventJson saveEvent) {
    final PipelineStopAndDeleteEventJson deleteEvent = new PipelineStopAndDeleteEventJson();
    deleteEvent.setName(saveEvent.getName());
    deleteEvent.setUser(saveEvent.getUser());
    deleteEvent.setRev(saveEvent.getRev());
    return deleteEvent;
  }

  /**
   * Creates a list of configurations to be inserted in the connection verifier stage
   *
   * @param connection The connection to preview
   * @return The new configurations for the verifier stage
   */
  public Map<String, ConnectionConfiguration> getVerifierConfigurationJson(ConnectionDefinitionPreviewJson connection) {
    List<Config> configs = BeanHelper.unwrapConfigConfiguration(connection.getConfiguration());
    Map<String, ConnectionConfiguration> connectionConfig = new HashMap<>();
    connectionConfig.put(
      connection.getConnectionId(),
      new ConnectionConfiguration(
          connection.getType(),
          Integer.valueOf(connection.getVersion()),
          configs
      )
    );
    return connectionConfig;
  }

  /**
   * Parses the DynamicPreviewRequestJson raw JSON into a ConnectionDefinitionPreviewJson object
   *
   * @param dynamicPreviewRequest the raw request JSON
   * @return The ConnectionDefinitionPreviewJson object representing the request parameters
   */
  public ConnectionDefinitionPreviewJson getConnectionPreviewJson(DynamicPreviewRequestJson dynamicPreviewRequest) {
    ConnectionDefinitionPreviewJson connection = null;
    try {
      connection = ObjectMapperFactory.get().readValue(
          ObjectMapperFactory.get().writeValueAsString(dynamicPreviewRequest.getParameters().get("connection")),
          ConnectionDefinitionPreviewJson.class
      );
      connection.setConnectionId(UUID.randomUUID().toString());
    } catch (Exception e) {
      LOG.error("Exception mapping connection definition", e);
    }
    return connection;
  }

  /**
   * Load the pipeline template raw JSON
   *
   * @return The pipeline template raw JSON
   * @throws IOException if the pipeline template file cannot be loaded
   */
  private String getTemplatePipelineDefinitionJson() throws IOException {
    return IOUtils.toString(
        this.getClass().getResource("/com/streamsets/datacollector/restapi/connection/rawDynamicPreviewTemplate.json"),
        Charsets.UTF_8
    );
  }
}
