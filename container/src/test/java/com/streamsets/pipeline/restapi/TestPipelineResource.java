package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.container.Pipeline;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import javax.ws.rs.core.Application;
import java.util.List;

/**
 * Created by harikiran on 10/19/14.
 */
public class TestPipelineResource extends JerseyTest {

  @Override
  protected Application configure() {
    return new ResourceConfig(PipelineResource.class);
  }

  @Test
  public void testGetAllPipelines() {
    List<Pipeline> r = target("/v1/pipelines").request().get(List.class);
    //returns a List of Maps, where each map corresponds to a module info object
    System.out.println(r.toString());
  }
}
