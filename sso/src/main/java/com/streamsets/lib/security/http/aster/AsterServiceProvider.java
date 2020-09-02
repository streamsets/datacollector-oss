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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.streamsets.datacollector.util.Configuration;

/**
 *  A Singleton provider for setting and providing the configured Aster Service
 */
public final class AsterServiceProvider {
  @VisibleForTesting
  static final String ASTER_URL_CONF = "aster.url";

  private volatile AsterService service = null;

  private static final AsterServiceProvider INSTANCE = new AsterServiceProvider();

  private AsterServiceProvider() {
  }

  /**
   * Get Aster Service Provider Singleton Instance
   * @return Aster Service provider
   */
  public static AsterServiceProvider getInstance() {
    return INSTANCE;
  }

  /**
   * Check whether Aster Service is enabled
   * @param appConf Application configuration
   * @return true if ASTER is enabled else false
   */
  public static boolean isEnabled(Configuration appConf) {
    return !Strings.isNullOrEmpty(appConf.get(ASTER_URL_CONF, ""));
  }

  /**
   * Registered Aster Service
   * @return Aster Service
   */
  public AsterService getService() {
    return service;
  }

  /**
   * Set the Aster Service to be provided by this provider
   * @param service aster service
   */
  public void set(AsterService service) {
    this.service = service;
  }

}
