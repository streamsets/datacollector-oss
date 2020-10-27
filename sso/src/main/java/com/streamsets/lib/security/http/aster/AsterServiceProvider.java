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
  public static final String ASTER_URL = "aster.url";
  // for local testing, use https://streamsets.dev:18632
  public static final String ASTER_URL_DEFAULT = "https://accounts.streamsets.com";

  private AsterServiceProvider() {
  }

  /**
   * Check whether Aster Service is enabled
   * @param appConf Application configuration
   * @return true if ASTER is enabled else false
   */
  public static boolean isEnabled(Configuration appConf) {
    return !Strings.isNullOrEmpty(appConf.get(ASTER_URL, ASTER_URL_DEFAULT));
  }

}
