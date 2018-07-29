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
package com.streamsets.datacollector.publicrestapi;

import com.streamsets.datacollector.event.binding.MessagingDtoJsonMapper;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.execution.Runner;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.CallbackInfoJson;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.DisconnectedSSOManager;
import com.streamsets.lib.security.http.DisconnectedSecurityInfo;
import com.streamsets.pipeline.api.impl.Utils;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;

@Path("/v1/cluster")
@DenyAll
public class PublicClusterResource {

  private final Manager manager;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public PublicClusterResource(Manager pipelineStateManager, RuntimeInfo runtimeInfo) {
    this.manager = pipelineStateManager;
    this.runtimeInfo = runtimeInfo;
  }

  void updateSlaveCallbackInfo(CallbackInfoJson callbackInfoJson) throws PipelineException {
    Runner runner = manager.getRunner(callbackInfoJson.getName(), callbackInfoJson.getRev());
    if (!runner.getState().getStatus().isActive()) {
      throw new RuntimeException(Utils.format("Pipeline '{}::{}' is not active, but is '{}'",
          callbackInfoJson.getName(), callbackInfoJson.getRev(), runner.getState().getStatus()));
    }
    runner.updateSlaveCallbackInfo(callbackInfoJson.getCallbackInfo());

  }

  @Deprecated
  @POST
  @Path("/callback")
  @PermitAll
  public Response callback(CallbackInfoJson callbackInfoJson) throws PipelineException {
    updateSlaveCallbackInfo(callbackInfoJson);
    return Response.ok().build();
  }

  @POST
  @Path("/callbackWithResponse")
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response callbackWithResponse(CallbackInfoJson callbackInfoJson) throws PipelineException, IOException {
    updateSlaveCallbackInfo(callbackInfoJson);
    File storeFile = new File(runtimeInfo.getDataDir(), DisconnectedSSOManager.DISCONNECTED_SSO_AUTHENTICATION_FILE);
    if (runtimeInfo.isDPMEnabled()) {
      DisconnectedSecurityInfo disconnectedSecurityInfo = DisconnectedSecurityInfo.fromJsonFile(storeFile);
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(MessagingDtoJsonMapper.INSTANCE.toJson(
          disconnectedSecurityInfo)).build();
    } else {
      return Response.ok().build();
    }

  }

}
