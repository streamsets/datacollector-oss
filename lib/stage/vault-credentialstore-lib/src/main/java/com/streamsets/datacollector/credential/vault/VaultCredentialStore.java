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
package com.streamsets.datacollector.credential.vault;

import com.google.common.base.Splitter;
import com.streamsets.datacollector.vault.Vault;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialStoreDef;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Credential store backed by Vault.
 */
@CredentialStoreDef(label = "Vault KeyStore")
public class VaultCredentialStore implements CredentialStore {
  private static final Logger LOG = LoggerFactory.getLogger(VaultCredentialStore.class);

  public static final String PATH_KEY_SEPARATOR_PROP = "pathKey.separator";
  public static final String PATH_KEY_SEPARATOR_DEFAULT = "&";
  public static final String SEPARATOR_OPTION = "separator";
  public static final String DELAY_OPTION = "delay";

  private String pathKeySeparator;
  private Vault vault;

  @Override
  public List<ConfigIssue> init(Context context) {
    List<ConfigIssue> issues = new ArrayList<>();
    pathKeySeparator = context.getConfig(PATH_KEY_SEPARATOR_PROP);
    if (pathKeySeparator == null) {
      pathKeySeparator = PATH_KEY_SEPARATOR_DEFAULT;
    }
    try {
      vault = createVault(context);
      vault.init();
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(Errors.VAULT_000, context.getId(), ex));
    }
    return issues;
  }

  protected Vault createVault(Context context) {
    return new Vault(context);
  }

  class VaultCredentialValue implements CredentialValue {
    private final String path;
    private final String key;
    private final long delay;

    public VaultCredentialValue(String path, String key, long delay) {
      this.path = path;
      this.key = key;
      this.delay = delay;
    }

    @Override
    public String get() throws StageException {
      LOG.debug("Getting '{}' delay '{}'", this, delay);
      return vault.read(path, key, delay);
    }

    @Override
    public String toString() {
      return "VaultCredentialValue{" + "path='" + path + '\'' + ", key='" + key + '\'' + '}';
    }

  }

  /*
   * Name format is Vault's [path]@[key]
   */
  @Override
  public CredentialValue get(String group, String name, String credentialStoreOptions) throws StageException {
    Utils.checkNotNull(group, "group cannot be NULL");
    Utils.checkNotNull(name, "name cannot be NULL");
    try {
      Map<String, String> optionsMap =
          Splitter.on(",").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(credentialStoreOptions);
      String separator = optionsMap.get(SEPARATOR_OPTION);
      if (separator == null) {
        separator = pathKeySeparator;
      }
      String[] splits = name.split(separator, 2);
      if (splits.length != 2) {
        throw new IllegalArgumentException(Utils.format("Vault CredentialStore name '{}' should be <path>{}<key>",
            name, separator
        ));
      }
      String delayStr = optionsMap.get(DELAY_OPTION);
      long delay = (delayStr == null) ? 0 : Long.parseLong(delayStr);

      CredentialValue credential = new VaultCredentialValue(splits[0], splits[1], delay);
      credential.get();
      return credential;
    } catch (Exception ex) {
      throw new StageException(Errors.VAULT_001, name, ex);
    }
  }

  @Override
  public void destroy() {
    vault.destroy();
  }

}
