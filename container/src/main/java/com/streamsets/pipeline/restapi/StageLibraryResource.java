/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.PipelineDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.container.LocaleInContext;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v1/definitions")
public class StageLibraryResource {
  private static final String DEFAULT_ICON_FILE = "PipelineDefinition-bundle.properties";
  private final StageLibraryTask stageLibrary;

  @Inject
  public StageLibraryResource(StageLibraryTask stageLibrary) {
    this.stageLibrary = stageLibrary;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDefinitions() {
    //The definitions to be returned
    Map<String, List<Object>> definitions = new HashMap<String, List<Object>>();

    //Populate the definitions with all the stage definitions
    List<StageDefinition> stageDefinitions = stageLibrary.getStages();
    List<Object> stages = new ArrayList<Object>(stageDefinitions.size());
    stages.addAll(stageDefinitions);
    definitions.put("stages", stages);

    //Populate the definitions with the PipelineDefinition
    List<Object> pipeline = new ArrayList<Object>(1);
    pipeline.add(new PipelineDefinition());
    definitions.put("pipeline", pipeline);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(definitions).build();
  }

  @GET
  @Path("/stages/icon")
  @Produces(MediaType.APPLICATION_SVG_XML)
  public Response getIcon(@QueryParam("name") String name,
                          @QueryParam("library") String library,
                          @QueryParam("version") String version) {
    StageDefinition stage = stageLibrary.getStage(library, name, version);
    String iconFile;
    if(stage.getIcon() != null && !stage.getIcon().isEmpty()) {
      iconFile = stage.getIcon();
    } else {
      iconFile = DEFAULT_ICON_FILE;
    }
    final InputStream resourceAsStream = stage.getStageClassLoader().getResourceAsStream(
      iconFile);
    return Response.ok().type(MediaType.APPLICATION_SVG_XML_TYPE).entity(resourceAsStream).build();
  }
}
