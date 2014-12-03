/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TestStageLibraryResource extends JerseyTest {

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new StageLibraryResourceConfig());
        register(StageLibraryResource.class);
      }
    };
  }
  static class StageLibraryResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(TestUtil.StageLibraryTestInjector.class).to(StageLibraryTask.class);
    }
  }

  @Test
  public void testGetAllModules() {

    Response response = target("/v1/definitions").request().get();
    Map<String, List<Object>> definitions = response.readEntity(new GenericType<Map<String, List<Object>>>() {});

    //check the pipeline definition
    Assert.assertTrue(definitions.containsKey("pipeline"));
    List<Object> pipelineDefinition = definitions.get("pipeline");
    Assert.assertTrue(pipelineDefinition.size() == 1);

    //check the stages
    Assert.assertTrue(definitions.containsKey("stages"));
    List<Object> stages = definitions.get("stages");
    Assert.assertEquals(2, stages.size());

    //TODO The json is deserialized as a generic map
    /*Assert.assertTrue(pipelineDefinition.get(0) instanceof PipelineDefinition);

    PipelineDefinition pd = (PipelineDefinition)pipelineDefinition.get(0);
    Assert.assertNotNull(pd.getConfigDefinitions());
    Assert.assertEquals(2, pd.getConfigDefinitions().size());*/

    /*//check the first stage
    Assert.assertTrue(stages.get(0) instanceof StageDefinition);
    StageDefinition s1 = (StageDefinition) stages.get(0);
    Assert.assertNotNull(s1);
    Assert.assertEquals("source", s1.getName());
    Assert.assertEquals("com.streamsets.pipeline.restapi.TestStageLibraryResource$TSource", s1.getClassName());
    Assert.assertEquals("1.0.0", s1.getVersion());
    Assert.assertEquals("label", s1.getLabel());
    Assert.assertEquals("description", s1.getDescription());
    Assert.assertEquals(4, s1.getConfigDefinitions().size());

    Assert.assertTrue(stages.get(1) instanceof StageDefinition);
    StageDefinition s2 = (StageDefinition) stages.get(1);
    Assert.assertNotNull(s2);
    Assert.assertEquals("target", s2.getName());
    Assert.assertEquals("com.streamsets.pipeline.restapi.TestStageLibraryResource$TTarget", s2.getClassName());
    Assert.assertEquals("1.0.0", s2.getVersion());
    Assert.assertEquals("label", s2.getLabel());
    Assert.assertEquals("description", s2.getDescription());
    Assert.assertTrue(s2.getConfigDefinitions().isEmpty());*/
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

}