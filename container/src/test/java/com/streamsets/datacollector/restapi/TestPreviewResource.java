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

import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.event.dto.Event;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.handler.EventHandlerTask;
import com.streamsets.datacollector.event.handler.remote.MockPreviewer;
import com.streamsets.datacollector.event.handler.remote.RemoteDataCollectorResult;
import com.streamsets.datacollector.event.json.DynamicPreviewEventJson;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.DynamicPreviewRequestWithOverridesJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.lib.security.http.RemoteSSOService;
import com.streamsets.lib.security.http.RestClient;
import com.streamsets.lib.security.http.SSOPrincipal;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;


@RunWith(PowerMockRunner.class)
@PrepareForTest({
    RemoteSSOService.class, RestClient.class
})
@PowerMockIgnore({
    "com.sun.crypto.*",
    "javax.crypto.*",
    "javax.security.*",
    "javax.management.*",
    "jdk.internal.reflect.*"
})
public class TestPreviewResource extends JerseyTest {

  private static Logger LOG = LoggerFactory.getLogger(TestPipelineStoreResource.class);

  static Manager manager = PowerMockito.mock(Manager.class);
  static Configuration configuration = PowerMockito.mock(Configuration.class);
  static SSOPrincipal principal =PowerMockito.mock(SSOPrincipal.class);
  static PipelineStoreTask store = PowerMockito.mock(PipelineStoreTask.class);
  static AclStoreTask aclStore = PowerMockito.mock(AclStoreTask.class);
  static RuntimeInfo runtimeInfo = PowerMockito.mock(RuntimeInfo.class);
  static UserGroupManager userGroupManager = PowerMockito.mock(UserGroupManager.class);
  static EventHandlerTask eventHandlerTask = PowerMockito.mock(EventHandlerTask.class);
  static PipelineStoreTask pipelineStoreTask = PowerMockito.mock(PipelineStoreTask.class);
  static StageLibraryTask stageLibraryTask = PowerMockito.mock(StageLibraryTask.class);
  static BlobStoreTask blobStoreTask = PowerMockito.mock(BlobStoreTask.class);

  static RestClient controlHubClient = PowerMockito.mock(RestClient.class);
  private DynamicPreviewRequestWithOverridesJson requestWithOverridesJson;

  {
    try {
      requestWithOverridesJson = new ObjectMapper().readValue(
              "{" + "\"stageOutputsToOverrideJson\":[]," + "\"dynamicPreviewRequestJson\":" + "{\"batchSize\":1," + "\"batches\":1,\"timeout\":10000,\"type\":\"PROTECTION_POLICY\"," + "\"parameters\":{\"read.policy" + ".id\":\"66999139-d2d8-44ca-82a7-707dfbc03a00:test\",\"write.policy" + ".id\":\"f6de0b15-f9b4-4e15-9e39-e8d9e2999be6:test\"," + "\"pipelineId\":\"a95f9210-0d0f-484b-b297" + "-3f252ae789d7:test\"," + "\"pipelineCommitId\":\"23e3dbbd-6f21-4f21-b31d-d50bc37ce505:test\"}}}",
              DynamicPreviewRequestWithOverridesJson.class
      );
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected Application configure() {

    PowerMockito.when(principal.getName()).thenReturn("User");
    PowerMockito.when(principal.getRoles()).thenReturn(new HashSet<String>(Arrays.asList("A", "B")));
    PowerMockito.when(principal.getGroups()).thenReturn(new HashSet<String>(Arrays.asList("A", "B")));
    PowerMockito.when(runtimeInfo.isDPMEnabled()).thenReturn(true);
    PowerMockito.when(runtimeInfo.isAclEnabled()).thenReturn(false);

    PowerMockito.mockStatic(RemoteSSOService.class);
    PowerMockito.when(RemoteSSOService.getValidURL(Matchers.anyString())).thenReturn("/controlhub");

    PowerMockito.mockStatic(RestClient.class);
    PowerMockito.mockStatic(RestClient.Builder.class);


    RestClient.Builder builder = PowerMockito.mock(RestClient.Builder.class, Mockito.RETURNS_DEEP_STUBS);

    try {
      PowerMockito.when(
          RestClient.builder(Matchers.anyString())
      ).thenReturn(builder);

      PowerMockito.when(builder
          .json(Matchers.anyBoolean())
          .appAuthToken(Matchers.anyString())
          .componentId(Matchers.anyString())
          .csrf(Matchers.anyBoolean())
          .jsonMapper(Matchers.any())
          .build(Matchers.anyString())
      ).thenReturn(controlHubClient);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new ResourceConfig() {
      {
        register(new PreviewResource(
            manager,
            configuration,
            principal,
            store,
            aclStore,
            runtimeInfo,
            userGroupManager,
            eventHandlerTask,
            pipelineStoreTask,
            blobStoreTask,
            stageLibraryTask
        ));
      }
    };
  }

  @Test
  public void dynamicPreviewShouldFailIfControlHubFailsToRespond() throws IOException {
    // Mock Control hub failure response
    DynamicPreviewEventJson dpEvent = new DynamicPreviewEventJson();

    RestClient.Response response = PowerMockito.mock(RestClient.Response.class);
    PowerMockito.when(response.successful()).thenReturn(false);

    PowerMockito.when(controlHubClient.post(Matchers.any())).thenReturn(response);

    // Call to control hub to initialize the dynamic preview
    try {
      target("/v1/pipeline/dynamicPreview").request()
          .post(Entity.json(requestWithOverridesJson));
      Assert.fail("Exception should be thrown");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getCause().getMessage().contains("PREVIEW_0104"));
    }
  }

  @Test
  public void dynamicPreviewShouldShowErrorIfControlHubFailsWithAnError() throws IOException {
    // Mock Control hub failure response
    DynamicPreviewEventJson dpEvent = new DynamicPreviewEventJson();

    RestClient.Response response = PowerMockito.mock(RestClient.Response.class);
    PowerMockito.when(response.successful()).thenReturn(false);
    PowerMockito.when((response.getError())).thenReturn(new HashMap<String, String>() {{ put("ISSUES", "SOME_ERROR"); }});

    PowerMockito.when(controlHubClient.post(Matchers.any())).thenReturn(response);

    // Call to control hub to initialize the dynamic preview
    try {
      target("/v1/pipeline/dynamicPreview").request()
          .post(Entity.json(requestWithOverridesJson));
      Assert.fail("Exception should be thrown");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getCause().getMessage().contains("PREVIEW_0104"));
      Assert.assertTrue(ex.getCause().getMessage().contains("SOME_ERROR"));
    }
  }

  @Test
  public void dynamicPreviewShouldFailIfPreviewIdDoesNotExist() throws IOException {
    // Mock Control hub success response with no pipelines ids
    DynamicPreviewEventJson dpEvent = new DynamicPreviewEventJson();

    RestClient.Response response = PowerMockito.mock(RestClient.Response.class);
    PowerMockito.when(response.successful()).thenReturn(true);
    PowerMockito.when(response.haveData()).thenReturn(true);
    PowerMockito.when((response.getData(DynamicPreviewEventJson.class))).thenReturn(dpEvent);

    PowerMockito.when(controlHubClient.post(Matchers.any())).thenReturn(response);

    // Call to control hub to initialize the dynamic preview
    try {
      target("/v1/pipeline/dynamicPreview").request()
          .post(Entity.json(requestWithOverridesJson));
      Assert.fail("Exception should be thrown");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getCause().getMessage().contains("PREVIEW_0103"));
    }
  }

  @Test
  public void dynamicPreviewShouldFailOnNullBody() throws IOException {
    try {
      target("/v1/pipeline/dynamicPreview").request()
          .post(Entity.json(null));
      Assert.fail("Exception should be thrown");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getCause().getMessage().contains("cannot be null"));
    }
  }

  @Test
  public void dynamicPreviewShouldFailOnNullRequest() throws IOException {
    try {
      target("/v1/pipeline/dynamicPreview").request()
          .post(Entity.json("{" + "\"stageOutputsToOverrideJson\":[] }"));
      Assert.fail("Exception should be thrown");
    } catch (Exception ex) {
      Assert.assertTrue(ex.getCause().getMessage().contains("cannot be null"));
    }
  }

  @Test
  public void testDynamicPreviewConnectionVerifier() throws IOException {
    RemoteDataCollectorResult okResult = RemoteDataCollectorResult.immediate("pipeline-id");
    PowerMockito
        .when(eventHandlerTask.handleLocalEvent(Mockito.any(Event.class), Mockito.any(EventType.class), Mockito.anyMap()))
        .thenReturn(okResult);
    PowerMockito
        .when(manager.getPreviewer(Mockito.anyString()))
        .thenReturn(new MockPreviewer("test", "test", "0", Collections.emptyList(), x -> null));

    String requestRawJson = IOUtils.toString(
        this.getClass().getResource("/com/streamsets/datacollector/restapi/connectionVerifierDynamicPreviewRequest.json"),
        Charsets.UTF_8
    );
    DynamicPreviewRequestWithOverridesJson connectionVerifierPreviewRequest = new ObjectMapper().readValue(
        requestRawJson,
        DynamicPreviewRequestWithOverridesJson.class
    );
    StageDefinition stageDef = Mockito.mock(StageDefinition.class);
    PowerMockito
        .when(stageLibraryTask.getStage(
            "streamsets-datacollector-aws-lib",
            "com_streamsets_pipeline_stage_common_s3_AwsS3ConnectionVerifier",
            false
            ))
        .thenReturn(stageDef);
    Mockito.when(stageDef.getVersion()).thenReturn(1);
    target("/v1/pipeline/dynamicPreview").request()
        .post(Entity.json(connectionVerifierPreviewRequest));
  }
}
