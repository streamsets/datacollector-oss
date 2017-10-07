/*
 * Copyright 2017 StreamSets Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.credential;

import com.google.common.base.Preconditions;
import com.streamsets.datacollector.security.GroupsInScope;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.List;

/**
 * CredentialStore proxy that enforces group belonging.
 * <p/>
 * This proxy is used to the corresponding credential store when the authorization has to be check (on user pipeline
 * start).
 */
public class GroupEnforcerCredentialStore implements CredentialStore {
  private String storeId;
  private final CredentialStore store;

  public GroupEnforcerCredentialStore(CredentialStore store) {
    Utils.checkNotNull(store, "credentialStore");
    this.store = store;
  }

  @Override
  public List<ConfigIssue> init(Context context) {
    storeId = context.getId();
    return store.init(context);
  }

  @Override
  public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
    Preconditions.checkNotNull(group, "group cannot be NULL");
    Preconditions.checkNotNull(name, "name cannot be NULL");
    if (!GroupsInScope.isUserGroupInScope(group)) {
      throw new StageException(Errors.CREDENTIAL_STORE_001, storeId, group, name);
    }
    return store.get(group, name, credentialStoreOptions);
  }

  @Override
  public void destroy() {
    store.destroy();
  }

}
