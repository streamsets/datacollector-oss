package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.stagelibrary.StageLibrary;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by harikiran on 10/18/14.
 */
@Path("/v1/stage-library")
public class StageLibraryResource {
  private StageLibrary stageLibrary;

  @Inject
  public StageLibraryResource(StageLibrary stageLibrary) {
    this.stageLibrary = stageLibrary;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllRegisteredModules() {
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(stageLibrary.getStages()).build();
  }

}
