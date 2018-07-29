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

import com.google.common.base.Splitter;
import com.streamsets.datacollector.config.CredentialStoreDefinition;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CredentialStoresTaskImpl extends AbstractTask implements CredentialStoresTask {
  private static final Logger LOG = LoggerFactory.getLogger(CredentialStoresTaskImpl.class);
  static final String VAULT_CREDENTIAL_STORE_KEY = "com.streamsets.datacollector.vaultELs.credentialStore";

  private final Configuration configuration;
  private final StageLibraryTask stageLibraryTask;
  private final Map<String, CredentialStore> stores;
  private final List<CredentialStoreDefinition> credentialStoreDefinitions;

  @Inject
  public CredentialStoresTaskImpl(
      Configuration configuration, StageLibraryTask stageLibraryTask
  ) {
    super("CredentialStoresTask");
    this.configuration = configuration;
    this.stageLibraryTask = stageLibraryTask;
    stores = new HashMap<>();
    credentialStoreDefinitions = new ArrayList<>();
  }

  Map<String, CredentialStore> getStores() {
    return stores;
  }

  @Override
  public List<CredentialStoreDefinition> getConfiguredStoreDefinititions() {
    return Collections.unmodifiableList(credentialStoreDefinitions);
  }


  @Override
  protected void initTask() {
    super.initTask();
    List<CredentialStore.ConfigIssue> issues = loadAndInitStores();
    if (!issues.isEmpty()) {
      throw new RuntimeException("Could not initialize credential stores: " + issues);
    }
    CredentialEL.setCredentialStores(getStores());

    String vaultELcredentialStoreId = configuration.get("vaultEL.credentialStore.id", null);
    if (vaultELcredentialStoreId != null) {
      CredentialStore store = getStores().get(vaultELcredentialStoreId);
      if (store == null) {
        throw new RuntimeException(Utils.format(
            "Vault EL functions CredentialStore '{}' is not defined",
            vaultELcredentialStoreId
        ));
      }
      DataCollectorServices.instance().put(VAULT_CREDENTIAL_STORE_KEY, store);
      LOG.warn(
          "Vault EL functions are deprecated. CredentialStore '{}' registered as vault EL functions implementation",
          vaultELcredentialStoreId
      );
    }
  }

  @Override
  protected void stopTask() {
    for (Map.Entry<String, CredentialStore> entry : getStores().entrySet()) {
      LOG.debug("Destroying CredentialStore '{}'", entry.getKey());
      try {
        entry.getValue().destroy();
      } catch (Exception ex) {
        LOG.warn("Error destroying CredentialStore '{}': {}", entry.getKey(), ex);
      }
    }
    super.stopTask();
  }

  protected List<CredentialStore.ConfigIssue> loadAndInitStores() {
    List<CredentialStore.ConfigIssue> issues = new ArrayList<>();

    Map<String, CredentialStoreDefinition> defs = new HashMap<>();
    for (CredentialStoreDefinition def : stageLibraryTask.getCredentialStoreDefinitions()) {
      defs.put(def.getStageLibraryDefinition().getName() + "::" + def.getName(), def);
    }

    String storeIds = configuration.get("credentialStores", "");
    for (String storeId : Splitter.on(",").omitEmptyStrings().trimResults().split(storeIds)) {
      LOG.debug("Initializing CredentialStore '{}'", storeId);
      String storeConfigPrefix = "credentialStore." + storeId + ".";
      Configuration storeDefConfig = configuration.getSubSetConfiguration(storeConfigPrefix);
      String defName = storeDefConfig.get(storeConfigPrefix + "def", null);
      if (defName == null) {
        throw new RuntimeException(Utils.format(
            "Missing CredentialStore configuration '{}'",
            storeConfigPrefix + ".def"
        ));
      }
      CredentialStoreDefinition storeDef = defs.get(defName);
      if (storeDef == null) {
        throw new RuntimeException(Utils.format("Missing CredentialStore definition '{}'", defName));
      }
      Configuration storeConfig = storeDefConfig.getSubSetConfiguration(storeConfigPrefix + "config.");

      CredentialStore.Context context = createContext(storeId, storeConfig);

      try {
        CredentialStore store = storeDef.getStoreClass().newInstance();
        store = new GroupEnforcerCredentialStore(store);
        store = new ClassloaderInContextCredentialStore(storeDef, store);
        issues.addAll(store.init(context));
        getStores().put(storeId, store);
        credentialStoreDefinitions.add(storeDef);
      } catch (Exception ex) {
        issues.add(context.createConfigIssue(Errors.CREDENTIAL_STORE_000, ex));
      }
    }
    return issues;
  }

  protected CredentialStore.Context createContext(
      String storeId,
      Configuration storeConfig
  ) {
    return new CredentialStore.Context() {
      @Override
      public String getId() {
        return storeId;
      }

      @Override
      public CredentialStore.ConfigIssue createConfigIssue(
          ErrorCode errorCode, Object... args
      ) {
        return new CredentialStore.ConfigIssue() {
          @Override
          public String toString() {
            return Utils.format(
                "{} - Store ID '{}', ",
                errorCode.toString(),
                Utils.format(errorCode.getMessage(), args)
            );
          }
        };
      }

      @Override
      public String getConfig(String configName) {
        return storeConfig.get("credentialStore." + storeId + ".config." + configName, null);
      }
    };
 }

}
