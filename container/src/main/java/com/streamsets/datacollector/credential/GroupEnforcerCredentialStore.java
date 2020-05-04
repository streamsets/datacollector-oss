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
import com.streamsets.pipeline.api.credential.ManagedCredentialStore;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CredentialStore proxy that enforces group belonging.
 * <p/>
 * This proxy is used to the corresponding credential store when the authorization has to be check (on user pipeline
 * start).
 */
public class GroupEnforcerCredentialStore<T extends CredentialStore> implements ManagedCredentialStore {
  private String storeId;
  private final T store;

  private boolean enforceEntryGroup;

  public GroupEnforcerCredentialStore(T store) {
    Utils.checkNotNull(store, "credentialStore");
    this.store = store;
  }

  @Override
  public List<ConfigIssue> init(Context context) {
    storeId = context.getId();
    this.enforceEntryGroup = Boolean.parseBoolean(context.getConfig("enforceEntryGroup"));
    return store.init(context);
  }

  @Override
  public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
    Preconditions.checkNotNull(group, "group cannot be NULL");
    Preconditions.checkNotNull(name, "name cannot be NULL");
    if (!GroupsInScope.isUserGroupInScope(group)) {
      throw new StageException(Errors.CREDENTIAL_STORE_001, storeId, group, name);
    }
    if (enforceEntryGroup && !hasGroupAccess(group, name, credentialStoreOptions)) {
      throw new StageException(Errors.CREDENTIAL_STORE_002, group, name, storeId);
    }
    return store.get(group, name, credentialStoreOptions);
  }

  @Override
  public void store(List<String> groups, String name, String credentialValue) throws StageException {
    CredentialStoresTask.checkManagedState(store);
    Preconditions.checkNotNull(name, "name cannot be NULL");
    ((ManagedCredentialStore)store).store(groups, name, credentialValue);
  }

  @Override
  public void delete(String name) throws StageException {
    CredentialStoresTask.checkManagedState(store);
    Preconditions.checkNotNull(name, "name cannot be NULL");
    ((ManagedCredentialStore)store).delete(name);
  }

  @Override
  public List<String> getNames() throws StageException {
    CredentialStoresTask.checkManagedState(store);
    return ((ManagedCredentialStore)store).getNames();
  }

  @Override
  public void destroy() {
    store.destroy();
  }

  private boolean hasGroupAccess(String group, String name, String credentialStoreOptions) {
    CredentialValue entryGroups;
    try {
      entryGroups = store.get(group, name + "-groups", credentialStoreOptions);
    } catch (StageException ex) {
      throw new StageException(Errors.CREDENTIAL_STORE_003, name, storeId);
    }
    Set<String> entryGroupSet = Arrays.stream(entryGroups.get().split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toSet());
    return entryGroupSet.contains(group);
  }

}
