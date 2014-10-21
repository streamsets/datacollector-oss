package com.streamsets.pipeline.restapi;


import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import javax.ws.rs.core.Application;

/**
 * Created by harikiran on 10/16/14.
 */
public class TestModuleResource extends JerseyTest {

  @Override
  protected Application configure() {
    return new ResourceConfig(ModuleResource.class);
  }

  @Test
  public void testGetAllModules() {
    String moduleConfiguration = target("/v1/modules").request()
      .get(String.class);
    System.out.println(moduleConfiguration);
  }
}