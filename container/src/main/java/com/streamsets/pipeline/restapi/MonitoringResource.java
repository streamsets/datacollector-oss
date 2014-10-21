package com.streamsets.pipeline.restapi;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by harikiran on 10/20/14.
 */
@Path("/v1/metrics")
public class MonitoringResource {
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMetrics() {
    //Mock implementation
    return Response.ok().type(MediaType.APPLICATION_JSON)
      .entity("metrics").build();
  }
}
