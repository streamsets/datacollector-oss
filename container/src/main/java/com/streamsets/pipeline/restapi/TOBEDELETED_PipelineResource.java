/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.container.PipelineRunner;
import com.streamsets.pipeline.container.RunOutput;
import com.streamsets.pipeline.util.MockConfigGenerator;

import javax.servlet.ServletContext;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

@Path("/v1/pipelinesxxx")
public class TOBEDELETED_PipelineResource {

  private static final String PIPELINE = "pipeline";
  private PipelineRunner getRunner(ServletContext context) {
    return (PipelineRunner) context.getAttribute(PIPELINE);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllPipelines(@Context ServletContext context) {
    //Mock implementation, return 2 pipeline configuration objects
    List<PipelineConfiguration> pipelineConfigurations = new ArrayList<PipelineConfiguration>(2);
    pipelineConfigurations.add(MockConfigGenerator.getRuntimePipelineConfiguration());
    pipelineConfigurations.add(MockConfigGenerator.getRuntimePipelineConfiguration());

    return Response.ok().type(MediaType.APPLICATION_JSON)
      .entity(pipelineConfigurations).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{pipelineName}/config")
  public Response getConfiguration(@Context ServletContext context,
                                   @PathParam("pipelineName") String pipelineName) {
    //test
    PipelineConfiguration r = MockConfigGenerator.getRuntimePipelineConfiguration();
    return Response.ok().type(MediaType.APPLICATION_JSON)
      .entity(r).build();
  }

  @POST
  @Path("/{pipelineName}/config")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setConfiguration(@Context ServletContext context,
              @PathParam("pipelineName") String pipelineName,
              PipelineConfiguration pipelineConfiguration,
              @QueryParam("mode") String mode) {
    //Mock implementation
    return Response.accepted().type(MediaType.APPLICATION_JSON).entity(pipelineConfiguration).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{pipelineName}/preview")
  public Response preview(@Context ServletContext context,
                          @PathParam("pipelineName") String pipelineName,
                          @QueryParam("offset") long offset,
                          @QueryParam("stages") String stages,
                          @QueryParam("batchSize") int batchSize) {
    //Mock implementation
    PipelineRunner runner = getRunner(context);
    RunOutput output = runner.preview(String.valueOf(offset));
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(output).build();
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/{pipelineName}/status")
  public Response getStatus(@Context ServletContext context,
    @PathParam("pipelineName") String pipelineName) {
    //Mock implementation
    return Response.ok().type(MediaType.TEXT_HTML)
      .entity("READY").build();
  }

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/{pipelineName}/status")
  public Response setStatus(@Context ServletContext context,
    @PathParam("pipelineName") String pipelineName,
                                    @QueryParam("status") String status) {
    //Mock implementation
    return Response.ok().type(MediaType.TEXT_HTML)
      .entity(status).build();
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/{pipelineName}/offset")
  public Response getOffset(@Context ServletContext context,
    @PathParam("pipelineName") String pipelineName) {
    //Mock implementation
    return Response.ok().type(MediaType.TEXT_HTML)
      .entity("0").build();
  }

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/{pipelineName}/offset")
  public Response setOffset(@Context ServletContext context,
      @PathParam("pipelineName") String pipelineName,
                                    @QueryParam("offset") String offset) {
    //Mock implementation
    return Response.ok().type(MediaType.TEXT_HTML)
      .entity(offset).build();
  }

  @POST
  @Path("/{pipelineName}/step")
  @Produces(MediaType.APPLICATION_JSON)
  public Response step(@Context ServletContext context,
                       @QueryParam("pipelineName") String pipelineName) {
    PipelineRunner runner = getRunner(context);
    RunOutput output = runner.preview(pipelineName);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(output).build();
  }

  @GET
  @Path("/{pipelineName}/sampling")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSamplingData(@Context ServletContext context,
                       @QueryParam("pipelineName") String pipelineName) {
    PipelineRunner runner = getRunner(context);
    RunOutput output = runner.preview(pipelineName);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(output).build();
  }

  @POST
  @Path("/{pipelineName}/sampling")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setSamplingConfiguration(@Context ServletContext context,
                       @QueryParam("pipelineName") String pipelineName,
                       String samplingConfiguration) {
    PipelineRunner runner = getRunner(context);
    RunOutput output = runner.preview(pipelineName);
    return Response.ok().type(MediaType.APPLICATION_JSON).entity(output).build();
  }

}
