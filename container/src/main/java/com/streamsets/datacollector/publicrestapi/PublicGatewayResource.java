/*
 * Copyright 2020 StreamSets Inc.
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

import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.GatewayBaseResource;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.gateway.GatewayInfo;
import com.streamsets.pipeline.api.impl.Utils;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.Path;

@Path("/v1/gateway")
@PermitAll
public class PublicGatewayResource extends GatewayBaseResource {

  @Inject
  public PublicGatewayResource(Manager manager, PipelineStoreTask store, RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
    this.store = store;
    this.manager = manager;
  }

  @Override
  protected void validateRequest(GatewayInfo gatewayInfo) throws PipelineException {
    Utils.checkState(!gatewayInfo.getNeedGatewayAuth(), Utils.format("Invalid request"));
    super.validateRequest(gatewayInfo);
  }

  @Override
  protected String getBaseUrlPath() {
    return "public-rest/";
  }

}
