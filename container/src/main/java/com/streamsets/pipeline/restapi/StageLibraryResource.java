/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.alerts.DataRuleEvaluator;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.PipelineDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.el.RuntimeEL;
import com.streamsets.pipeline.restapi.bean.BeanHelper;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
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
@DenyAll
public class StageLibraryResource {
  private static final String DEFAULT_ICON_FILE = "PipelineDefinition-bundle.properties";
  private static final String PNG_MEDIA_TYPE = "image/png";
  private static final String SVG_MEDIA_TYPE = "image/svg+xml";

  @VisibleForTesting
  static final String STAGES = "stages";
  @VisibleForTesting
  static final String PIPELINE = "pipeline";
  @VisibleForTesting
  static final String RULES_EL_METADATA = "rulesElMetadata";
  @VisibleForTesting
  static final String EL_CONSTANT_DEFS = "elConstantDefinitions";
  @VisibleForTesting
  static final String EL_FUNCTION_DEFS = "elFunctionDefinitions";
  @VisibleForTesting
  static final String RUNTIME_CONFIGS = "runtimeConfigs";

  private final StageLibraryTask stageLibrary;

  @Inject
  public StageLibraryResource(StageLibraryTask stageLibrary) {
    this.stageLibrary = stageLibrary;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getDefinitions() {
    //The definitions to be returned
    Map<String, Object> definitions = new HashMap<>();

    //Populate the definitions with all the stage definitions
    List<StageDefinition> stageDefinitions = stageLibrary.getStages();
    List<Object> stages = new ArrayList<>(stageDefinitions.size());
    stages.addAll(BeanHelper.wrapStageDefinitions(stageDefinitions));
    definitions.put(STAGES, stages);

    //Populate the definitions with the PipelineDefinition
    List<Object> pipeline = new ArrayList<>(1);
    pipeline.add(BeanHelper.wrapPipelineDefinition(PipelineDefinition.getPipelineDef()));
    definitions.put(PIPELINE, pipeline);

    Map<String, Object> map = new HashMap<>();
    map.put(EL_FUNCTION_DEFS, BeanHelper.wrapElFunctionDefinitions(DataRuleEvaluator.getElEvaluator().getElFunctionDefinitions()));
    map.put(EL_CONSTANT_DEFS, BeanHelper.wrapElConstantDefinitions(DataRuleEvaluator.getElEvaluator().getElConstantDefinitions()));
    definitions.put(RULES_EL_METADATA, map);

    definitions.put(RUNTIME_CONFIGS, RuntimeEL.getRuntimeConfKeys());

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(definitions).build();
  }

  @GET
  @Path("/stages/icon")
  @Produces({SVG_MEDIA_TYPE, PNG_MEDIA_TYPE})
  @PermitAll
  public Response getIcon(@QueryParam("name") String name,
                          @QueryParam("library") String library,
                          @QueryParam("version") String version) {
    StageDefinition stage = Utils.checkNotNull(stageLibrary.getStage(library, name, version),
      Utils.formatL("Could not find stage library: {}, name: {}, version: {}", library, name, version));
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
