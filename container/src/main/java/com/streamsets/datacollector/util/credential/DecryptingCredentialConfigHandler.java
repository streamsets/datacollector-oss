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
import com.streamsets.pipeline.api.credential.CredentialValue;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Optional;

/**
 * Handler for decrypting the credential configuration value from the managed credential store
 */
class DecryptingCredentialConfigHandler implements CredentialConfigHandler {
  private final CredentialStoresTask credentialStoresTask;
  private final Configuration configuration;

  DecryptingCredentialConfigHandler(CredentialStoresTask credentialStoresTask, Configuration configuration) {
    this.credentialStoresTask = credentialStoresTask;
    this.configuration = configuration;
  }

  @Override
  public Object handleCredential(Pair<ConfigDefinition, Object> configDefinitionValue, ConfigContext configContext) {
    return Optional.ofNullable(credentialStoresTask.getDefaultManagedCredentialStore()).map(managedCredentialStore -> {
      boolean isManagedCredential = configDefinitionValue.getRight()
          .toString().startsWith(
              String.format(
                  PipelineCredentialHandler.CREDENTIAL_EL_PREFIX_TEMPLATE,
                  configuration.get(CredentialStoresTask.MANAGED_DEFAULT_CREDENTIAL_STORE_CONFIG, ""))
          );
      if (isManagedCredential) {
        String secretName = PipelineCredentialHandler.generateSecretName(configContext.getPipelineId(),
            configContext.getStageId(),
            configContext.getConfigName()
        );
        CredentialValue value = managedCredentialStore.get(CredentialStoresTask.DEFAULT_SDC_GROUP, secretName, "");
        return Optional.ofNullable((Object) value.get()).orElse("");
      }
      return null;
    }).orElse(configDefinitionValue.getRight());
  }
}
