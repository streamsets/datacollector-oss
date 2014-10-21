package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.ModuleRegistry;
import com.streamsets.pipeline.util.MockConfigGenerator;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by harikiran on 10/18/14.
 */
@Path("/v1/modules")
public class ModuleResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllRegisteredModules() {
    ModuleRegistry moduleConfiguration = MockConfigGenerator.getStaticModuleConfig();
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(moduleConfiguration).build();
  }
}
