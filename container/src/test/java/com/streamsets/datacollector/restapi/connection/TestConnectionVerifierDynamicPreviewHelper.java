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

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.dynamicpreview.DynamicPreviewRequestJson;
import com.streamsets.datacollector.dynamicpreview.DynamicPreviewType;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.json.DynamicPreviewEventJson;
import com.streamsets.datacollector.event.json.PipelineSaveEventJson;
import com.streamsets.datacollector.event.json.PipelineStopAndDeleteEventJson;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.ConfigConfigurationJson;
import com.streamsets.datacollector.restapi.bean.ConnectionDefinitionPreviewJson;
import com.streamsets.datacollector.restapi.bean.PipelineEnvelopeJson;
import com.streamsets.datacollector.restapi.bean.StageConfigurationJson;
import com.streamsets.datacollector.restapi.bean.UserJson;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TestConnectionVerifierDynamicPreviewHelper {

  private ConnectionVerifierDynamicPreviewHelper verifierPreviewHelper;
  private DynamicPreviewRequestJson dynamicPreviewRequest;
  private UserJson currentUser;

  @Before
  public void setUp() {
    verifierPreviewHelper = new ConnectionVerifierDynamicPreviewHelper();
    dynamicPreviewRequest = getDynamicPreviewRequest();
    currentUser = getTestUserJson();
  }

  private DynamicPreviewRequestJson getDynamicPreviewRequest() {
    DynamicPreviewRequestJson dynamicPreviewRequest = new DynamicPreviewRequestJson();
    dynamicPreviewRequest.setBatches(2);
    dynamicPreviewRequest.setBatchSize(10);
    dynamicPreviewRequest.setTimeout(1000);
    dynamicPreviewRequest.setType(DynamicPreviewType.CONNECTION_VERIFIER);
    dynamicPreviewRequest.setTestOrigin(false);
    dynamicPreviewRequest.setSkipTargets(false);
    dynamicPreviewRequest.setSkipLifecycleEvents(true);
    ArrayList<ConfigConfigurationJson> config = new ArrayList<>();
    config.add(new ConfigConfigurationJson("awsConfig.credentialMode", "WITH_CREDENTIALS"));
    config.add(new ConfigConfigurationJson("awsConfig.awsAccessKeyId", "test-key"));
    config.add(new ConfigConfigurationJson("awsConfig.awsSecretAccessKey", "test-secret"));
    ConnectionDefinitionPreviewJson connection = new ConnectionDefinitionPreviewJson(
        "1",
        "AWS_S3",
        config,
        "com.streamsets.pipeline.stage.common.s3.AwsS3ConnectionVerifier",
        "connection",
        "streamsets-datacollector-aws-lib"
    );

    Map<String, Object> params = new HashMap<>();
    params.put("connection", connection);
    dynamicPreviewRequest.setParameters(params);
    return dynamicPreviewRequest;
  }

  private UserJson getTestUserJson() {
    UserJson userJson = new UserJson();
    userJson.setName("admin");
    userJson.setGroups(ImmutableList.of("admin"));
    return userJson;
  }

  @Test
  public void testGetVerifierDynamicPreviewPipeline() throws IOException {
    PipelineEnvelopeJson verifierPipeline = verifierPreviewHelper.getVerifierDynamicPreviewPipeline(dynamicPreviewRequest);

    // assert pipeline configuration
    Assert.assertTrue(verifierPipeline.getPipelineConfig().getPipelineId().contains("-Dynamic-Preview"));
    Assert.assertEquals(2, verifierPipeline.getPipelineConfig().getStages().size());
    Assert.assertEquals(
        "com_streamsets_pipeline_stage_destination_devnull_NullDTarget",
        verifierPipeline.getPipelineConfig().getStages().get(1).getStageName()
    );
    Assert.assertEquals(
        "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget",
        verifierPipeline.getPipelineConfig().getErrorStage().getStageName()
    );

    // assert verifier stage configuration
    StageConfigurationJson verifierStage = verifierPipeline.getPipelineConfig().getStages().get(0);
    Assert.assertEquals("AmazonS3ConnectionVerifier_01", verifierStage.getInstanceName());
    Assert.assertEquals("streamsets-datacollector-aws-lib", verifierStage.getLibrary());
    Assert.assertEquals("com_streamsets_pipeline_stage_common_s3_AwsS3ConnectionVerifier", verifierStage.getStageName());
    Assert.assertEquals("WITH_CREDENTIALS", verifierStage.getConfig("connection.awsConfig.credentialMode").getValue());
    Assert.assertEquals("test-key", verifierStage.getConfig("connection.awsConfig.awsAccessKeyId").getValue());
    Assert.assertEquals("test-secret", verifierStage.getConfig("connection.awsConfig.awsSecretAccessKey").getValue());
    Assert.assertEquals(ImmutableList.of(), verifierStage.getInputLanes());
    Assert.assertEquals(ImmutableList.of("DevRawDataSource_01OutputLane15397448822820"), verifierStage.getOutputLanes());
    Assert.assertEquals(ImmutableList.of(), verifierStage.getEventLanes());
    Assert.assertEquals(ImmutableList.of(), verifierStage.getServices());
  }

  @Test
  public void testGetVerifierDynamicPreviewEvent() throws IOException {
    PipelineEnvelopeJson verifierPipeline = verifierPreviewHelper.getVerifierDynamicPreviewPipeline(dynamicPreviewRequest);
    String pipelineId = verifierPipeline.getPipelineConfig().getPipelineId();
    DynamicPreviewEventJson dynamicPreviewEvent = verifierPreviewHelper
        .getVerifierDynamicPreviewEvent(verifierPipeline, dynamicPreviewRequest, currentUser);
    Assert.assertNotNull(dynamicPreviewEvent.getPreviewEvent());
    Assert.assertEquals(1, dynamicPreviewEvent.getBeforeActions().size());
    Assert.assertEquals(1, dynamicPreviewEvent.getAfterActions().size());

    // Assert preview event
    Assert.assertEquals(EventType.PREVIEW_PIPELINE.getValue(), dynamicPreviewEvent.getPreviewEventTypeId());
    Assert.assertEquals(pipelineId, dynamicPreviewEvent.getPreviewEvent().getName());
    Assert.assertEquals("admin", dynamicPreviewEvent.getPreviewEvent().getUser());
    Assert.assertEquals("0", dynamicPreviewEvent.getPreviewEvent().getRev());
    Assert.assertEquals(2, dynamicPreviewEvent.getPreviewEvent().getBatches());
    Assert.assertEquals(10, dynamicPreviewEvent.getPreviewEvent().getBatchSize());
    Assert.assertEquals(false, dynamicPreviewEvent.getPreviewEvent().isTestOrigin());
    Assert.assertEquals(1000, dynamicPreviewEvent.getPreviewEvent().getTimeoutMillis());
    Assert.assertEquals(false, dynamicPreviewEvent.getPreviewEvent().isSkipTargets());
    Assert.assertEquals(true, dynamicPreviewEvent.getPreviewEvent().isSkipLifecycleEvents());

    // Assert save pipeline before action
    Assert.assertEquals(dynamicPreviewEvent.getBeforeActions().size(), dynamicPreviewEvent.getBeforeActionsEventTypeIds().size());
    Assert.assertEquals(
        Integer.valueOf(EventType.SAVE_PIPELINE.getValue()),
        dynamicPreviewEvent.getBeforeActionsEventTypeIds().get(0)
    );
    Assert.assertTrue(dynamicPreviewEvent.getBeforeActions().get(0) instanceof PipelineSaveEventJson);
    PipelineSaveEventJson saveEvent = (PipelineSaveEventJson) dynamicPreviewEvent.getBeforeActions().get(0);
    Assert.assertEquals(
        ObjectMapperFactory.get().writeValueAsString(verifierPipeline.getPipelineConfig()),
        saveEvent.getPipelineConfigurationAndRules().getPipelineConfig()
    );
    Assert.assertEquals(
        ObjectMapperFactory.get().writeValueAsString(verifierPipeline.getPipelineRules()),
        saveEvent.getPipelineConfigurationAndRules().getPipelineRules()
    );
    Assert.assertEquals(pipelineId, saveEvent.getName());
    Assert.assertEquals("admin", saveEvent.getUser());
    Assert.assertEquals("0", saveEvent.getRev());

    // Assert delete pipeline after action
    Assert.assertEquals(dynamicPreviewEvent.getAfterActions().size(), dynamicPreviewEvent.getAfterActionsEventTypeIds().size());
    Assert.assertEquals(
        Integer.valueOf(EventType.STOP_DELETE_PIPELINE.getValue()),
        dynamicPreviewEvent.getAfterActionsEventTypeIds().get(0)
    );
    Assert.assertTrue(dynamicPreviewEvent.getAfterActions().get(0) instanceof PipelineStopAndDeleteEventJson);
    PipelineStopAndDeleteEventJson deleteEvent = (PipelineStopAndDeleteEventJson) dynamicPreviewEvent.getAfterActions().get(0);
    Assert.assertEquals(pipelineId, deleteEvent.getName());
    Assert.assertEquals("admin", deleteEvent.getUser());
    Assert.assertEquals("0", deleteEvent.getRev());
  }
}
