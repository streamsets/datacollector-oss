/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.Configuration;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/configuration")
public class ConfigurationResource {
  private static final String UI_PREFIX = "ui.";

  private final Configuration config;

  @Inject
  public ConfigurationResource(Configuration configuration) {
    this.config = configuration;
  }

  @GET
  @Path("/ui")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUIConfiguration() throws PipelineStoreException {

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(config.getSubSetConfiguration(UI_PREFIX)).build();
  }


  @GET
  @Path("/all")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConfiguration() throws PipelineStoreException {

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(config.getSubSetConfiguration("")).build();
  }
}
