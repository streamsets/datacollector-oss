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
package com.streamsets.datacollector.credential;

import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.task.Task;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.ManagedCredentialStore;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collections;
import java.util.List;

/**
 * A credential store task is a task supposed to load credential stores configured in datacollector
 */
public interface CredentialStoresTask extends Task {
  String MANAGED_DEFAULT_CREDENTIAL_STORE_CONFIG = "auto.managed.default.credentialStores";

  String SSH_PUBLIC_KEY_SECRET = "sdc/defaultPublicKey";
  String DEFAULT_SDC_GROUP = "all";
  List<String> DEFAULT_SDC_GROUP_AS_LIST = Collections.singletonList(DEFAULT_SDC_GROUP);
  String PIPELINE_CREDENTIAL_PREFIX = "PIPELINE_VAULT_";

  /**
   * Get a list of configured store definitions
   * @return the list of configured credential store definitions
   */
  List<CredentialStoreDefinition> getConfiguredStoreDefinititions();

  /**
   * Get default managed credential store
   */
  ManagedCredentialStore getDefaultManagedCredentialStore();

  /**
   * Check whether the credential store class is
   * @param credentialStore a credential store implementation
   */
  static <T extends CredentialStore> void checkManagedState(T credentialStore) {
    Utils.checkState(ManagedCredentialStore.class.isAssignableFrom(credentialStore.getClass()), "Not a managed credential store");
  }
}
