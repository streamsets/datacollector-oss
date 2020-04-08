/*
 * Copyright 2017 StreamSets Inc.
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

import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.PipelineException;

import com.streamsets.lib.security.http.DisconnectedAuthentication;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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

@Path("/v1/system")
@Api(value = "system")
@DenyAll
@RequiresCredentialsDeployed
public class InfoResource {

  private final BuildInfo buildInfo;
  private final RuntimeInfo runtimeInfo;
  private final StatsCollector statsCollector;
  private final UserGroupManager userGroupManager;

  @Inject
  public InfoResource(
      BuildInfo buildInfo, RuntimeInfo runtimeInfo, StatsCollector statsCollector, UserGroupManager
      userGroupManager) {
    this.buildInfo = buildInfo;
    this.runtimeInfo = runtimeInfo;
    this.statsCollector = statsCollector;
    this.userGroupManager = userGroupManager;
  }

  @GET
  @Path("/info")
  @ApiOperation(value = "Returns SDC Info", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getBuildInfo() throws PipelineException, IOException {
    return Response.status(Response.Status.OK).entity(buildInfo).build();
  }

  @GET
  @Path("/info/currentUser")
  @ApiOperation(value = "Returns User Info", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getUserInfo(@Context SecurityContext context) throws PipelineException, IOException {
    Map<String, Object> map = new HashMap<>();
    String user;
    List<String> roles = new ArrayList<>();
    List<String> groups = new ArrayList<>();
    Principal principal = context.getUserPrincipal();

    if(principal != null) {
      user = principal.getName();
      if (context.isUserInRole(AuthzRole.GUEST) || context.isUserInRole(AuthzRole.GUEST_REMOTE)) {
        roles.add(AuthzRole.GUEST);
      }
      if (context.isUserInRole(AuthzRole.MANAGER) || context.isUserInRole(AuthzRole.MANAGER_REMOTE)) {
        roles.add(AuthzRole.MANAGER);
      }
      if (context.isUserInRole(AuthzRole.CREATOR) || context.isUserInRole(AuthzRole.CREATOR_REMOTE)) {
        roles.add(AuthzRole.CREATOR);
      }
      if (context.isUserInRole(AuthzRole.ADMIN) || context.isUserInRole(AuthzRole.ADMIN_REMOTE)) {
        roles.add(AuthzRole.ADMIN);
      } else if (context.isUserInRole(AuthzRole.ADMIN_ACTIVATION)) {
        roles.add(AuthzRole.ADMIN_ACTIVATION);
      }
      if (context.isUserInRole(DisconnectedAuthentication.DISCONNECTED_MODE_ROLE)) {
        roles.add(DisconnectedAuthentication.DISCONNECTED_MODE_ROLE);
      }
    } else {
      //In case of http.authentication=none
      user = "admin";
      roles.add(AuthzRole.ADMIN);
    }

    UserJson userJson = userGroupManager.getUser(principal);

    map.put("user", user);
    map.put("roles", roles);
    map.put("groups", userJson != null ? userJson.getGroups() : null);
    return Response.status(Response.Status.OK).entity(map).build();
  }

  @GET
  @Path("/info/serverTime")
  @ApiOperation(value = "Returns Server Time", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getServerTime(@Context SecurityContext context) throws PipelineException, IOException {
    Map<String, Object> map = new HashMap<>();
    map.put("serverTime", System.currentTimeMillis());
    return Response.status(Response.Status.OK).entity(map).build();
  }

  @GET
  @Path("/info/remote")
  @ApiOperation(value = "Returns Remote Server Info", response = Map.class,
      authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getRemoteInfo(@Context SecurityContext context) throws PipelineException, IOException {
    Map<String, Object> map = new HashMap<>();
    map.put("registrationStatus", runtimeInfo.isRemoteRegistrationSuccessful());
    return Response.status(Response.Status.OK).entity(map).build();
  }

  @GET
  @Path("/info/id")
  @ApiOperation(value = "SDC id", response = Map.class, authorizations = @Authorization(value = "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getSdcId(@Context SecurityContext context) throws PipelineException, IOException {
    Map<String, Object> map = new HashMap<>();
    map.put("id", runtimeInfo.getId());
    return Response.status(Response.Status.OK).entity(map).build();
  }

  @GET
  @Path("/stats")
  @ApiOperation(value = "Stats status", response = Map.class, authorizations = @Authorization(value =
      "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response getStatus() throws PipelineException, IOException {
    Map map = new HashMap();
    map.put("opted", statsCollector.isOpted());
    map.put("active", statsCollector.isActive());
    if (statsCollector.isActive()) {
      map.put("stats", statsCollector.getStatsInfo().snapshot());
    }
    return Response.status(Response.Status.OK).entity(map).build();
  }

  @POST
  @Path("/stats")
  @ApiOperation(value = "Stats status change", response = Map.class, authorizations = @Authorization(value =
      "basic"))
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  @RolesAllowed({AuthzRole.ADMIN, AuthzRole.ADMIN_REMOTE})
  public Response setStatus(@QueryParam("active") boolean active) throws PipelineException, IOException {
    statsCollector.setActive(active);
    Map map = new HashMap();
    map.put("opted", statsCollector.isOpted());
    map.put("active", statsCollector.isActive());
    if (statsCollector.isActive()) {
      map.put("stats", statsCollector.getStatsInfo().snapshot());
    }
    return Response.status(Response.Status.OK).entity(map).build();
  }

}
