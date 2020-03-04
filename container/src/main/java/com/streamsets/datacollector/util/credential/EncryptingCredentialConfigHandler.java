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
package com.streamsets.datacollector.util.credential;

import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.credential.CredentialStoresTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ElUtil;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Optional;

/**
 * Handler for encrypting the credential configuration value to the managed credential store
 */
class EncryptingCredentialConfigHandler implements CredentialConfigHandler {
  private final String managedCredentialName;
  private final CredentialStoresTask credentialStoreTask;

  EncryptingCredentialConfigHandler(
      CredentialStoresTask credentialStoreTask,
      Configuration configuration
  ) {
    this.credentialStoreTask = credentialStoreTask;
    this.managedCredentialName = configuration.get(CredentialStoresTask.MANAGED_DEFAULT_CREDENTIAL_STORE_CONFIG, "");
  }

  @Override
  public Object handleCredential(Pair<ConfigDefinition, Object> configDefinitionValue, ConfigContext configContext) {
    return Optional.ofNullable(credentialStoreTask.getDefaultManagedCredentialStore()).map(managedCredentialStore -> {
      boolean isElAndNotPlainText = ElUtil.isElString(configDefinitionValue.getRight());
      if (!isElAndNotPlainText) {
        String secretName = PipelineCredentialHandler.generateSecretName(configContext.getPipelineId(),
            configContext.getStageId(),
            configContext.getConfigName()
        );
        managedCredentialStore.store(
            CredentialStoresTask.DEFAULT_SDC_GROUP_AS_LIST,
            secretName,
            (String) configDefinitionValue.getRight()
        );
        return (Object) String.format(
            PipelineCredentialHandler.CREDENTIAL_EL_TEMPLATE,
            managedCredentialName,
            CredentialStoresTask.DEFAULT_SDC_GROUP,
            secretName
        );
      }
      return null;
    }).orElse(configDefinitionValue.getRight());
  }
}
