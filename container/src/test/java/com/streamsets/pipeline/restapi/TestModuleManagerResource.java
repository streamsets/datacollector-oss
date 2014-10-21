package com.streamsets.pipeline.restapi;


import com.streamsets.pipeline.container.ModuleInfo;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import javax.ws.rs.core.Application;
import java.util.List;

/**
 * Created by harikiran on 10/16/14.
 */
public class TestModuleManagerResource extends JerseyTest {

  @Override
  protected Application configure() {
    return new ResourceConfig(ModuleManagerResource.class);
  }

  @Test
  public void testGetAllModules() {
    List<ModuleInfo> r = target("/v1/modules").request().get(List.class);
    //returns a List of Maps, where each map corresponds to a module info object
    System.out.println(r.toString());
  }
}