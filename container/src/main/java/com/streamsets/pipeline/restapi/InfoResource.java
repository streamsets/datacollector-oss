/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.util.PipelineException;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v1/info")
@DenyAll
public class InfoResource {

  private final BuildInfo buildInfo;

  @Inject
  public InfoResource(BuildInfo buildInfo) {
    this.buildInfo = buildInfo;
  }

  @GET
  @Path("/sdc")
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getBuild() throws PipelineException, IOException {
    return Response.status(Response.Status.OK).entity(buildInfo).build();
  }

  @GET
  @Path("/user")
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getUser(@Context SecurityContext context) throws PipelineException, IOException {
    String user = context.getUserPrincipal().getName();
    List<String> roles = new ArrayList<>();
    if (context.isUserInRole(AuthzRole.GUEST)) {
      roles.add(AuthzRole.GUEST);
    }
    if (context.isUserInRole(AuthzRole.MANAGER)) {
      roles.add(AuthzRole.MANAGER);
    }
    if (context.isUserInRole(AuthzRole.CREATOR)) {
      roles.add(AuthzRole.CREATOR);
    }
    if (context.isUserInRole(AuthzRole.ADMIN)) {
      roles.add(AuthzRole.ADMIN);
    }
    Map<String, Object> map = new HashMap<>();
    map.put("user", user);
    map.put("roles", roles);
    return Response.status(Response.Status.OK).entity(map).build();
  }

}
