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
package com.streamsets.datacollector.restapi.configuration;

import com.streamsets.datacollector.store.AclStoreTask;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

public class AclStoreInjector implements Factory<AclStoreTask> {
  public static final String ACL_STORE = "acl-store";
  private AclStoreTask aclStore;

  @Inject
  public AclStoreInjector(HttpServletRequest request) {
    aclStore = (AclStoreTask) request.getServletContext().getAttribute(ACL_STORE);
  }

  @Override
  public AclStoreTask provide() {
    return aclStore;
  }

  @Override
  public void dispose(AclStoreTask pipelineStore) {
  }

}
