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

import com.google.common.collect.Lists;
import com.streamsets.datacollector.config.ConnectionDefinition;
import com.streamsets.datacollector.definition.ConnectionVerifierDefinition;
import com.streamsets.datacollector.el.RuleELRegistry;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.ConnectionDefinitionJson;
import com.streamsets.datacollector.restapi.bean.ConnectionsJson;
import com.streamsets.datacollector.restapi.configuration.JsonConfigurator;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestStageLibraryResource extends JerseyTest {

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(JsonConfigurator.class);
        register(new StageLibraryResourceConfig());
        register(StageLibraryResource.class);
        register(MultiPartFeature.class);
      }
    };
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetAllModules() {

    Response response = target("/v1/definitions").request().get();
    Map<String, Object> definitions = response.readEntity(new GenericType<Map<String, Object>>() {});

    //check the pipeline definition
    Assert.assertTrue(definitions.containsKey(StageLibraryResource.PIPELINE));
    List<Object> pipelineDefinition = (List<Object>)definitions.get(StageLibraryResource.PIPELINE);
    Assert.assertNotNull(pipelineDefinition);
    Assert.assertTrue(pipelineDefinition.size() == 1);

    //check the stages
    Assert.assertTrue(definitions.containsKey(StageLibraryResource.STAGES));
    List<Object> stages = (List<Object>)definitions.get(StageLibraryResource.STAGES);
    Assert.assertNotNull(stages);
    Assert.assertEquals(2, stages.size());

    //check the rules El metadata
    Assert.assertTrue(definitions.containsKey(StageLibraryResource.RULES_EL_METADATA));
    Map<String, Map> rulesElMetadata = (Map<String, Map>)definitions.get(StageLibraryResource.RULES_EL_METADATA);
    Assert.assertNotNull(rulesElMetadata);
    Assert.assertTrue(rulesElMetadata.containsKey(RuleELRegistry.GENERAL));
    Assert.assertTrue(rulesElMetadata.containsKey(RuleELRegistry.DRIFT));
    Assert.assertTrue(rulesElMetadata.get(RuleELRegistry.GENERAL).containsKey(StageLibraryResource.EL_FUNCTION_DEFS));
    Assert.assertTrue(rulesElMetadata.get(RuleELRegistry.GENERAL).containsKey(StageLibraryResource.EL_CONSTANT_DEFS));
    Assert.assertTrue(rulesElMetadata.get(RuleELRegistry.DRIFT).containsKey(StageLibraryResource.EL_FUNCTION_DEFS));
    Assert.assertTrue(rulesElMetadata.get(RuleELRegistry.DRIFT).containsKey(StageLibraryResource.EL_CONSTANT_DEFS));
  }

  @Test
  public void testGetIcon() throws IOException {
    Response response = target("/v1/definitions/stages/icon").queryParam("name", "target")
        .queryParam("library", "library").queryParam("version", "1.0.0").request().get();
    Assert.assertTrue(response.getEntity() != null);
  }

  @Test
  public void testGetDefaultIcon() throws IOException {
    Response response = target("/v1/definitions/stages/icon").queryParam("name", "source")
        .queryParam("library", "library").queryParam("version", "1.0.0").request().get();
    Assert.assertTrue(response.getEntity() != null);
  }


  @Test
  public void testGetLibraries() throws IOException {
    Response response = target("/v1/stageLibraries/list").request().get();
    Assert.assertTrue(response.getEntity() != null);
  }

  @Test
  public void testGetConnections() throws IOException {
    Response response = target("/v1/definitions/connections").request().get();
    Assert.assertNotNull(response.getEntity());
    response.readEntity(ConnectionsJson.class); // make sure the entity is the expected ConnectionsJson type
  }

  @Test
  public void testGetConnectionsVerifiers() {
    StageLibraryTask stageLibraryTask = Mockito.mock(StageLibraryTask.class);
    BuildInfo buildInfo = Mockito.mock(BuildInfo.class);
    Mockito.when(buildInfo.getVersion()).thenReturn("3.19.0");
    StageLibraryResource resource = new StageLibraryResource(
        stageLibraryTask,
        buildInfo,
        Mockito.mock(RuntimeInfo.class)
    );

    ConnectionDefinition conDef1 = Mockito.mock(ConnectionDefinition.class);
    Mockito.when(conDef1.getType()).thenReturn("type1");
    ConnectionDefinition conDef2 = Mockito.mock(ConnectionDefinition.class);
    Mockito.when(conDef2.getType()).thenReturn("type2");
    ConnectionDefinition conDef3 = Mockito.mock(ConnectionDefinition.class);
    Mockito.when(conDef3.getType()).thenReturn("type3");
    Mockito.when(stageLibraryTask.getConnections()).thenReturn(Lists.newArrayList(conDef1, conDef2, conDef3));
    Mockito.when(stageLibraryTask.getConnectionVerifiers("type1")).thenReturn(Sets.newSet(
        Mockito.mock(ConnectionVerifierDefinition.class)
    ));
    Mockito.when(stageLibraryTask.getConnectionVerifiers("type2")).thenReturn(Sets.newSet(
        Mockito.mock(ConnectionVerifierDefinition.class), Mockito.mock(ConnectionVerifierDefinition.class)
    ));
    Mockito.when(stageLibraryTask.getConnectionVerifiers("type3")).thenReturn(Collections.emptySet());

    Response response = resource.getConnections();
    Assert.assertNotNull(response.getEntity());
    ConnectionsJson json = (ConnectionsJson) response.getEntity();
    Assert.assertEquals(3, json.getConnections().size());
    ConnectionDefinitionJson json1 = json.getConnections().get(0);
    Assert.assertEquals("type1", json1.getType());
    Assert.assertEquals(1, json1.getVerifierDefinitions().size());
    ConnectionDefinitionJson json2 = json.getConnections().get(1);
    Assert.assertEquals("type2", json2.getType());
    Assert.assertEquals(2, json2.getVerifierDefinitions().size());
    ConnectionDefinitionJson json3 = json.getConnections().get(2);
    Assert.assertEquals("type3", json3.getType());
    Assert.assertEquals(0, json3.getVerifierDefinitions().size());

  }
}
