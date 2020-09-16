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
package com.streamsets.lib.security.http.aster;

import java.util.Collection;

/**
 *  Aster SSO Service Interface provided for integration
 */
public interface AsterService {

  /**
   * Returns if the engine is registered with Aster and with good standing tokens.
   */
  boolean isEngineRegistered();

  /**
   *  Get Rest Client for interacting with the SSOService
   * @return Rest client
   */
  AsterRestClient getRestClient();

  /**
   * Get Configuration for Aster Service
   * @return Aster Service Configuration
   */
  AsterConfiguration getConfig();

  /**
   *  Register callback hooks for Aster Service Events
   * @param hooks Hooks
   */
  void registerHooks(Collection<AsterServiceHook> hooks);
}
