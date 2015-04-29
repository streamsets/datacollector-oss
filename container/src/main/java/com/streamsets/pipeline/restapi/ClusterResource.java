/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.PipelineManager;
import com.streamsets.pipeline.restapi.bean.CallbackInfoJson;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

@Path("/v1/cluster")
@DenyAll
public class ClusterResource {

  private final PipelineManager pipelineStateManager;

  @Inject
  public ClusterResource(PipelineManager pipelineStateManager) {
    this.pipelineStateManager = pipelineStateManager;
  }

  @POST
  @Path("/callback")
  @PermitAll
  public Response callback(CallbackInfoJson callbackInfoJson) {
    pipelineStateManager.updateSlaveCallbackInfo(callbackInfoJson.getCallbackInfo());
    return Response.ok().build();
  }
}
