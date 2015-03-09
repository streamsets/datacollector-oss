/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.PipelineDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;

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
  private static final String PNG_MEDIA_TYPE = "image/png";
  private static final String SVG_MEDIA_TYPE = "image/svg+xml";
  private final StageLibraryTask stageLibrary;

  @Inject
  public StageLibraryResource(StageLibraryTask stageLibrary) {
    this.stageLibrary = stageLibrary;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDefinitions() {
    //The definitions to be returned
    Map<String, Object> definitions = new HashMap<>();

    //Populate the definitions with all the stage definitions
    List<StageDefinition> stageDefinitions = stageLibrary.getStages();
    List<Object> stages = new ArrayList<>(stageDefinitions.size());
    stages.addAll(BeanHelper.wrapStageDefinitions(stageDefinitions));
    definitions.put("stages", stages);

    //Populate the definitions with the PipelineDefinition
    List<Object> pipeline = new ArrayList<>(1);
    pipeline.add(BeanHelper.wrapPipelineDefinition(new PipelineDefinition(stageLibrary)));
    definitions.put("pipeline", pipeline);

    if(stageLibrary.getElMetadata() != null) {
      definitions.put("elMetadata", BeanHelper.wrapElMetadata(stageLibrary.getElMetadata()));
    }

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(definitions).build();
  }

  @GET
  @Path("/stages/icon")
  @Produces({SVG_MEDIA_TYPE, PNG_MEDIA_TYPE})
  public Response getIcon(@QueryParam("name") String name,
                          @QueryParam("library") String library,
                          @QueryParam("version") String version) {
    StageDefinition stage = stageLibrary.getStage(library, name, version);
    String iconFile = DEFAULT_ICON_FILE;
    String responseType = SVG_MEDIA_TYPE;

    if(stage.getIcon() != null && !stage.getIcon().isEmpty()) {
      iconFile = stage.getIcon();
    }

    final InputStream resourceAsStream = stage.getStageClassLoader().getResourceAsStream(iconFile);

    if(iconFile.endsWith(".svg"))
      responseType = SVG_MEDIA_TYPE;
    else if(iconFile.endsWith(".png"))
      responseType = PNG_MEDIA_TYPE;

    return Response.ok().type(responseType).entity(resourceAsStream).build();
  }
}
