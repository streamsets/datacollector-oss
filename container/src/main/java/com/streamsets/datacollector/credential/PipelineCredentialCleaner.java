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
package com.streamsets.datacollector.credential;

import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.StateEventListener;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.dc.execution.manager.standalone.ThreadUsage;
import com.streamsets.pipeline.api.credential.ManagedCredentialStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class PipelineCredentialCleaner implements StateEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineCredentialCleaner.class);

  private ManagedCredentialStore managedCredentialStore;

  PipelineCredentialCleaner(ManagedCredentialStore managedCredentialStore) {
    this.managedCredentialStore = managedCredentialStore;
  }

  @Override
  public synchronized void onStateChange(
      PipelineState fromState,
      PipelineState toState,
      String toStateJson,
      ThreadUsage threadUsage,
      Map<String, String> offset
  ) throws PipelineException {
    String pipelineSecretPrefixToCheck = (CredentialStoresTask.PIPELINE_CREDENTIAL_PREFIX + toState.getPipelineId()).toLowerCase();
    if (toState.getStatus() == PipelineStatus.DELETED) {
      LOG.info("Triggered clean up of managed credentials for pipeline {}", toState.getPipelineId());
      Optional.ofNullable(managedCredentialStore)
          .ifPresent(m -> m.getNames()
                  .stream()
                  .filter(n -> n.toLowerCase().startsWith(pipelineSecretPrefixToCheck))
                  .forEach(managedCredentialStore::delete)
          );
    }
  }
}
