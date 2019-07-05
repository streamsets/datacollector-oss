/*
 * Copyright 2019 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.validation.PipelineConfigurationValidator;
import com.streamsets.pipeline.api.impl.Utils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1")
@Api(value = "upgrader")
public class PipelineUpgraderResource {

  private final StageLibraryTask stageLibrary;

  @Inject
  public PipelineUpgraderResource(StageLibraryTask stageLibrary) {
    this.stageLibrary = stageLibrary;
  }

  @Path("/pipeline-upgrader")
  @POST
  @ApiOperation(value = "Upgrades a pipeline for the current SDC", response = PipelineConfigurationJson.class)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response upgrade(@ApiParam(name = "pipeline", required = true) PipelineConfigurationJson pipeline) {
    PipelineConfiguration pipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipeline);
    String name = pipelineConfig.getTitle();
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipelineConfig);
    pipelineConfig = validator.validate();
    return Response.ok().entity(BeanHelper.wrapPipelineConfiguration(pipelineConfig)).build();
  }

}
