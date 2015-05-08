/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.callback.CallbackInfo;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.restapi.bean.CallbackInfoJson;
import com.streamsets.pipeline.util.AuthzRole;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Path("/v1/cluster")
@DenyAll
public class ClusterResource {

  private final PipelineManager pipelineManager;

  @Inject
  public ClusterResource(PipelineManager pipelineStateManager) {
    this.pipelineManager = pipelineStateManager;
  }

  @POST
  @Path("/callback")
  @PermitAll
  public Response callback(CallbackInfoJson callbackInfoJson) {
    pipelineManager.updateSlaveCallbackInfo(callbackInfoJson.getCallbackInfo());
    return Response.ok().build();
  }


  @GET
  @Path("/redirectToSlave")
  @PermitAll
  public Response redirectToSlaveInstance(@QueryParam("sdcURL") String sdcURL,
      @Context final HttpServletResponse response,
      @Context SecurityContext context) throws IOException {

    Collection<CallbackInfo> callbackInfoCollection = pipelineManager.getSlaveCallbackList();
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
