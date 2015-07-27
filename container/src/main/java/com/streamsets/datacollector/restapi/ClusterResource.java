/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi;

import com.google.common.base.Joiner;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.restapi.bean.CallbackInfoJson;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.pipeline.api.impl.Utils;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Path("/v1/cluster")
@DenyAll
public class ClusterResource {

  private final Manager manager;
  private final String user;

  @Inject
  public ClusterResource(Manager pipelineStateManager, Principal user) {
    this.manager = pipelineStateManager;
    this.user = user.getName();
  }

  @POST
  @Path("/callback")
  @PermitAll
  public Response callback(CallbackInfoJson callbackInfoJson) throws PipelineStoreException {
    Runner runner = manager.getRunner(callbackInfoJson.getUser(), callbackInfoJson.getName(), callbackInfoJson.getRev());
    if (runner.getState().getStatus() != PipelineStatus.RUNNING) {
      throw new RuntimeException(Utils.format("Pipeline '{}::{}' is not running, but is '{}'", callbackInfoJson.getName(), callbackInfoJson.getRev(),
        runner.getState().getStatus()));
    }
    runner.updateSlaveCallbackInfo(callbackInfoJson.getCallbackInfo());
    return Response.ok().build();
  }


  @GET
  @Path("/redirectToSlave")
  @PermitAll
  public Response redirectToSlaveInstance(
    @QueryParam("name") String name,
    @QueryParam("rev") @DefaultValue("0") String rev,
    @QueryParam("sdcURL") String sdcURL,
    @Context final HttpServletResponse response,
    @Context SecurityContext context) throws IOException, PipelineStoreException {
    Runner runner = manager.getRunner(user, name, rev);
    Collection<CallbackInfo> callbackInfoCollection = runner.getSlaveCallbackList();
    CallbackInfo slaveCallbackInfo = null;

    for(CallbackInfo callbackInfo : callbackInfoCollection) {
      if(sdcURL.equals(callbackInfo.getSdcURL())) {
        slaveCallbackInfo = callbackInfo;
      }
    }

    if(slaveCallbackInfo != null) {
      String user;
      List<String> authTokens = new ArrayList<>();
      Principal principal = context.getUserPrincipal();

      if(principal != null) {
        user = principal.getName();
        if (context.isUserInRole(AuthzRole.GUEST)) {
          authTokens.add(slaveCallbackInfo.getGuestToken());
        }
        if (context.isUserInRole(AuthzRole.MANAGER)) {
          authTokens.add(slaveCallbackInfo.getManagerToken());
        }
        if (context.isUserInRole(AuthzRole.CREATOR)) {
          authTokens.add(slaveCallbackInfo.getCreatorToken());
        }
        if (context.isUserInRole(AuthzRole.ADMIN)) {
          authTokens.add(slaveCallbackInfo.getAdminToken());
        }
      } else {
        //In case of http.authentication=none
        user = "admin";
        authTokens.add(slaveCallbackInfo.getAdminToken());
      }

      Joiner joiner = Joiner.on( "," ).skipNulls();
      String slaveURL = slaveCallbackInfo.getSdcURL() + "?auth_user=" + user + "&auth_token=" + joiner.join(authTokens);

      response.sendRedirect(slaveURL);

    } else {
      throw new RuntimeException("No Slave Instance found with URL - " + sdcURL);
    }

    return Response.ok().build();
  }

}