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

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialStore;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Map;

/**
 * EL functions to retrieve credentials, from a credential store, for external systems.
 * <p/>
 * The credential store must be configured in the SDC configuration. Different implementations may be available.
 * Multiple credential stores may be used at the same time, each credential store has an ID that uniquely identifies it.
 */
public class CredentialEL {
  private static Map<String, CredentialStore> credentialStores;

  public static void setCredentialStores(Map<String, CredentialStore> stores) {
    credentialStores = stores;
  }

  @VisibleForTesting
  static Map<String, CredentialStore> getCredentialStores() {
    Utils.checkState(credentialStores != null, "credentialStores not set");
    return credentialStores;
  }

  private static final String PREFIX = "credential";

  private CredentialEL() {
  }

  /**
   * Retrieves a credential from a credential store with default options.
   *
   * @param storeId credential store ID.
   * @param userGroup user group the user must belong to have access to the credential.
   * @param name reference name for the credential.
   * @return The credential value or throws an exception if the credential does not exists.
   * @throws StageException thrown if the credential value does not exist or if user is not authorized to retrieve
   * the credential.
   */
  @ElFunction(
      prefix = PREFIX,
      name = "get",
      description =
          "Retrieves the credential of the specified key frmn the specified store. " +
          "The user must belong the specified group",
      implicitOnly = true
  )
  public static CredentialValue get(
      @ElParam("storeId") String storeId,
      @ElParam("userGroup") String userGroup,
      @ElParam("name") String name
  ) {
    CredentialStore store = getCredentialStores().get(storeId);
    if (store == null) {
      throw new RuntimeException(String.format("Undefined '%s' credential store", storeId));
    }
    try {
      return store.get(userGroup, name, "");
    } catch (StageException ex) {
      return CredentialEL._throw(ex);
    }
  }

  /**
   * Retrieves a credential from a credential store with default options.
   *
   * @param storeId credential store ID.
   * @param userGroup user group the user must belong to have access to the credential.
   * @param name reference name for the credential.
   * @param options options specific to the credential store implementation.
   * @return The credential value or throws an exception if the credential does not exists.
   * @throws StageException thrown if the credential value does not exist or if user is not authorized to retrieve
   * the credential.
   */
  @ElFunction(
      prefix = PREFIX,
      name = "getWithOptions",
      description =
          "Retrieves the credential of the specified key from the specified store with store specific options. " +
          "The user must belong the specified group.",
      implicitOnly = true
  )
  public static CredentialValue getWithOptions(
      @ElParam("storeId") String storeId,
      @ElParam("userGroup") String userGroup,
      @ElParam("name") String name,
      @ElParam("storeOptions") String options
  ) {
    CredentialStore store = getCredentialStores().get(storeId);
    if (store == null) {
      throw new RuntimeException(String.format("Undefined '%s' credential store", storeId));
    }
    try {
      return store.get(userGroup, name, options);
    } catch (StageException ex) {
      return _throw(ex);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T, E extends Exception> T _throw(Throwable e) throws E {
    throw (E)e;
  }

}
