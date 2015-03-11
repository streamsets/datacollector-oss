/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
    Map<String, Object> rulesElMetadata = (Map<String, Object>)definitions.get(StageLibraryResource.RULES_EL_METADATA);
    Assert.assertNotNull(rulesElMetadata);
    Assert.assertTrue(rulesElMetadata.containsKey(StageLibraryResource.EL_FUNCTION_DEFS));
    Assert.assertTrue(rulesElMetadata.containsKey(StageLibraryResource.EL_CONSTANT_DEFS));

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