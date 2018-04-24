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
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.ext.DataCollectorServices;

@Deprecated
public class VaultEL {
  public static final String PREFIX = "vault";
  private static final String VAULT_CREDENTIAL_STORE_KEY = "com.streamsets.datacollector.vaultELs.credentialStore";

  private static final CredentialStore vaultCredentialStore;

  static {
    DataCollectorServices services = DataCollectorServices.instance();
    vaultCredentialStore = services.get(VAULT_CREDENTIAL_STORE_KEY);
  }

  private VaultEL() {}

  @ElFunction(
      prefix = PREFIX,
      name = "read",
      description = "Retrieves the value of the specified path in Vault."
  )
  public static String read(@ElParam("path") String path, @ElParam("key") String key) {
    return read(path, key, 0);
  }

  @ElFunction(
      prefix = PREFIX,
      name = "readWithDelay",
      description = "Retrieves the value of the specified path in Vault and waits for delay milliseconds." +
          "Primarily for AWS since generated credentials can take 5-10 seconds before they are ready for use."
  )
  public static String read(@ElParam("path") String path, @ElParam("key") String key, @ElParam("delay") long delay) {
    if (vaultCredentialStore == null) {
      throw new RuntimeException("There is no VaultCredentialStore configured for use with the vault EL functions");
    }
    try {
      return vaultCredentialStore.get("all", path + "@" + key, "delay=" + delay).get();
    } catch (StageException ex) {
      throw new RuntimeException(ex);
    }
  }

}
