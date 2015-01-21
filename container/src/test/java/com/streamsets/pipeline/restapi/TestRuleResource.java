/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.AlertDefinition;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

public class TestRuleResource  extends JerseyTest {

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";

  @Test
  public void testGetAlertsAPI() {
    Response r = target("/v1/rules/myPipeline/alert").queryParam("rev", PIPELINE_REV)
      .request().get();
    Assert.assertNotNull(r);
    List<AlertDefinition> alerts = r.readEntity(new GenericType<List<AlertDefinition>>() {});
    Assert.assertEquals(3, alerts.size());
  }

  @Test
  public void testSaveAlertsAPI() {
    List<AlertDefinition> alerts = new ArrayList<>();
    alerts.add(new AlertDefinition("a1", "a1", "a", "2", null, true));
    alerts.add(new AlertDefinition("a2", "a1", "a", "2", null, true));
    alerts.add(new AlertDefinition("a3", "a1", "a", "2", null, true));

    Response r = target("/v1/rules/myPipeline/alert").queryParam("rev", PIPELINE_REV).request()
      .post(Entity.json(alerts));
    List<AlertDefinition> result = r.readEntity(new GenericType<List<AlertDefinition>>() {});
    Assert.assertEquals(3, result.size());


  }

  /*********************************************/
  /*********************************************/

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new RuleResourceConfig());
        register(RuleResource.class);
      }
    };
  }

  static class RuleResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(TestUtil.PipelineManagerTestInjector.class).to(ProductionPipelineManagerTask.class);
    }
  }
}
