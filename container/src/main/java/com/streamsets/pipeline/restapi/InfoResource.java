/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.util.AuthzRole;
import com.streamsets.pipeline.util.PipelineException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;

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
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/v1/info")
@Api(value = "info")
@DenyAll
public class InfoResource {

  private final BuildInfo buildInfo;

  @Inject
  public InfoResource(BuildInfo buildInfo) {
    this.buildInfo = buildInfo;
  }

  @GET
  @Path("/sdc")
  @ApiOperation(value = "Returns SDC Info", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getBuildInfo() throws PipelineException, IOException {
    return Response.status(Response.Status.OK).entity(buildInfo).build();
  }

  @GET
  @Path("/user")
  @ApiOperation(value = "Returns User Info", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getUserInfo(@Context SecurityContext context) throws PipelineException, IOException {
    Map<String, Object> map = new HashMap<>();
    String user;
    List<String> roles = new ArrayList<>();
    Principal principal = context.getUserPrincipal();

    if(principal != null) {
      user = principal.getName();
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
    } else {
      //In case of http.authentication=none
      user = "admin";
      roles.add(AuthzRole.ADMIN);
    }

    map.put("user", user);
    map.put("roles", roles);
    return Response.status(Response.Status.OK).entity(map).build();
  }

}
