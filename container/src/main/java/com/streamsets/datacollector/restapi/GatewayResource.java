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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.execution.AclManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.bean.UserJson;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.store.impl.AclPipelineStoreTask;
import com.streamsets.datacollector.util.AuthzRole;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.pipeline.api.gateway.GatewayInfo;
import com.streamsets.pipeline.api.impl.Utils;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Path;
import java.security.Principal;

@Path("/v1/gateway")
@RolesAllowed({
    AuthzRole.MANAGER,
    AuthzRole.ADMIN,
    AuthzRole.MANAGER_REMOTE,
    AuthzRole.ADMIN_REMOTE
})
public class GatewayResource extends GatewayBaseResource {

  @Inject
  public GatewayResource(
      Manager manager,
      Principal principal,
      PipelineStoreTask store,
      AclStoreTask aclStore,
      RuntimeInfo runtimeInfo,
      UserGroupManager userGroupManager
  ) {
    this.runtimeInfo = runtimeInfo;
    UserJson currentUser;
    if (runtimeInfo.isDPMEnabled() && !runtimeInfo.isRemoteSsoDisabled()) {
      currentUser = new UserJson((SSOPrincipal)principal);
    } else {
      currentUser = userGroupManager.getUser(principal);
    }

    if (runtimeInfo.isAclEnabled()) {
      this.store = new AclPipelineStoreTask(store, aclStore, currentUser);
      this.manager = new AclManager(manager, aclStore, currentUser);
    } else {
      this.store = store;
      this.manager = manager;
    }
  }

  @Override
  protected void validateRequest(GatewayInfo gatewayInfo) throws PipelineException {
    Utils.checkState(gatewayInfo.getNeedGatewayAuth(), Utils.format("Invalid request"));
    super.validateRequest(gatewayInfo);
  }

  @Override
  protected String getBaseUrlPath() {
    return "rest/";
  }
}
