/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.definition.ELDefinitionExtractor;
import com.streamsets.datacollector.el.RuntimeEL;
import com.streamsets.datacollector.execution.alerts.DataRuleEvaluator;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.DefinitionsJson;
import com.streamsets.datacollector.restapi.bean.PipelineDefinitionJson;
import com.streamsets.datacollector.restapi.bean.StageDefinitionJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.api.impl.Utils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Path("/v1/definitions")
@Api(value = "definitions")
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

  @VisibleForTesting
  static final String EL_CATALOG = "elCatalog";

  private final StageLibraryTask stageLibrary;

  @Inject
  public StageLibraryResource(StageLibraryTask stageLibrary) {
    this.stageLibrary = stageLibrary;
  }

  @GET
  @ApiOperation(value = "Returns pipeline & stage configuration definitions", response = DefinitionsJson.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getDefinitions() {
    //The definitions to be returned
    DefinitionsJson definitions = new DefinitionsJson();

    //Populate the definitions with all the stage definitions
    List<StageDefinition> stageDefinitions = stageLibrary.getStages();
    List<StageDefinitionJson> stages = new ArrayList<>(stageDefinitions.size());
    stages.addAll(BeanHelper.wrapStageDefinitions(stageDefinitions));
    definitions.setStages(stages);

    //Populate the definitions with the PipelineDefinition
    List<PipelineDefinitionJson> pipeline = new ArrayList<>(1);
    pipeline.add(BeanHelper.wrapPipelineDefinition(stageLibrary.getPipeline()));
    definitions.setPipeline(pipeline);

    Map<String, Object> map = new HashMap<>();
    map.put(EL_FUNCTION_DEFS,DataRuleEvaluator.getElFunctionIdx());
    map.put(EL_CONSTANT_DEFS, DataRuleEvaluator.getElConstantIdx());
    definitions.setRulesElMetadata(map);

    map = new HashMap<>();
    map.put(EL_FUNCTION_DEFS,
            BeanHelper.wrapElFunctionDefinitionsIdx(ELDefinitionExtractor.get().getElFunctionsCatalog()));
    map.put(EL_CONSTANT_DEFS,
            BeanHelper.wrapElConstantDefinitionsIdx(ELDefinitionExtractor.get().getELConstantsCatalog()));
    definitions.setElCatalog(map);

    definitions.setRuntimeConfigs(RuntimeEL.getRuntimeConfKeys());

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(definitions).build();
  }

  @GET
  @Path("/stages/{library}/{stageName}/icon")
  @ApiOperation(value = "Return stage icon for library and stage name", response = Object.class,
    authorizations = @Authorization(value = "basic"))
  @Produces({SVG_MEDIA_TYPE, PNG_MEDIA_TYPE})
  @PermitAll
  public Response getIcon(@PathParam("library") String library, @PathParam("stageName") String name) {
    StageDefinition stage = Utils.checkNotNull(stageLibrary.getStage(library, name, false),
      Utils.formatL("Could not find stage library: {}, name: {}", library, name));
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
