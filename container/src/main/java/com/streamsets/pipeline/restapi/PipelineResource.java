package com.streamsets.pipeline.restapi;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.container.*;

import javax.servlet.ServletContext;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by harikiran on 10/19/14.
 */
@Path("/v1/pipelines")
public class PipelineResource {

  private static final String PIPELINE = "pipeline";
  private PipelineRunner getRunner(ServletContext context) {
    return (PipelineRunner) context.getAttribute(PIPELINE);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllPipelines(@Context ServletContext context) {
    //Mock implementation
    return Response.ok().type(MediaType.APPLICATION_JSON)
      .entity("All pipeLines").build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{pipelineName}/config")
  public Response getConfiguration(@Context ServletContext context,
                                   @PathParam("pipelineName") String pipelineName) {
    //test
    Pipeline p = createPipeline(true);
    return Response.ok().type(MediaType.APPLICATION_JSON)
      .entity(p).build();
  }

  @POST
  @Path("/{pipelineName}/config")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setConfiguration(@Context ServletContext context,
              @PathParam("pipelineName") String pipelineName,
              Pipeline pipeline,
              @QueryParam("mode") String mode) {
    //Mock implementation
    return Response.accepted().type(MediaType.APPLICATION_JSON).entity(pipeline).build();
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

  //Used for mock implementation, to be removed.
  private Pipeline createPipeline(boolean complete) {
    MetricRegistry metrics = new MetricRegistry();

    Source.Info sourceInfo = new ModuleInfo("s", "1", "S", "si");
    Source source = new ContainerModule.TSource();
    Pipeline.Builder pb = new Pipeline.Builder(metrics, sourceInfo, source, ImmutableSet.of("lane"));

    Processor.Info processorInfo = new ModuleInfo("p", "1", "P", "pi");
    Processor processor = new ContainerModule.TProcessor();
    pb.add(processorInfo, processor, ImmutableSet.of("lane"), ImmutableSet.of("lane"));

    if (complete) {
      Target.Info targetInfo = new ModuleInfo("t", "1", "T", "ti");
      Target target = new ContainerModule.TTarget();
      pb.add(targetInfo, target, ImmutableSet.of("lane"));
    }
    return (complete) ? pb.build() : pb.buildPreview();
  }

}
