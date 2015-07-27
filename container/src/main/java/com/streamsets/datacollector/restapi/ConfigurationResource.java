/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.Configuration;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;

@Path("/v1/configuration")
@Api(value = "configuration")
@DenyAll
public class ConfigurationResource {
  private static final String UI_PREFIX = "ui.";

  private final Configuration config;

  @Inject
  public ConfigurationResource(Configuration configuration) {
    this.config = configuration;
  }

  @GET
  @Path("/ui")
  @ApiOperation(value = "Returns UI SDC Configuration", response = Map.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getUIConfiguration() throws PipelineStoreException {

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(config.getSubSetConfiguration(UI_PREFIX)).build();
  }


  @GET
  @Path("/all")
  @ApiOperation(value = "Returns ALL SDC Configuration", response = Map.class,
    authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getConfiguration() throws PipelineStoreException {

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(config.getUnresolvedConfiguration()).build();
  }
}
