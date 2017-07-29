/**
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
package com.streamsets.datacollector.credential;

import com.google.common.base.Preconditions;
import com.streamsets.lib.security.http.HeadlessSSOPrincipal;
import com.streamsets.lib.security.http.SSOPrincipal;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;

import javax.security.auth.Subject;
import java.security.AccessController;
import java.util.List;
import java.util.Set;

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
    CredentialValue value;
    Subject subject = Subject.getSubject(AccessController.getContext());
    Set<SSOPrincipal> principals = subject.getPrincipals(SSOPrincipal.class);
    if (principals.isEmpty()) {
      throw new RuntimeException("ERROR: Invocation without setting user groups in context");
    }
    if (principals.size() > 1) {
      throw new RuntimeException("ERROR: Invocation with more than one principal in context: " + principals);
    }
    SSOPrincipal principal = principals.iterator().next();

    // if principal is headless & recovery we don't enforce groups because it is retry, restart or slave and we
    // already asserted group on the initial start.
    if (principal instanceof HeadlessSSOPrincipal && ((HeadlessSSOPrincipal)principal).isRecovery()) {
      value = store.get(group, name, credentialStoreOptions);
    } else if (principal.getGroups().contains(group)) {
      value = store.get(group, name, credentialStoreOptions);
    } else {
      throw new StageException(Errors.CREDENTIAL_STORE_001, storeId, group, name);
    }
    return value;
  }

  @Override
  public void destroy() {
    store.destroy();
  }

}
