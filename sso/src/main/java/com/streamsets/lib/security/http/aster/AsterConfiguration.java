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

/**
 *  Configuration for {@link AsterService}
 */
public interface AsterConfiguration {
  /**
   * Aster Base URL
   * @return Base URL
   */
  String getBaseUrl();

  /**
   * Returns the engine registration page URL.
   */
  String getEngineRegistrationPath();

  /**
   * Returns the engine registration REST endpoint URL.
   */
  String getRegistrationUrlRestPath();

  /**
   * Returns the engine login page URL.
   */
  String getUserLoginPath();

  /**
   * Returns the engine login REST endpoint URL.
   */
  String getUserLoginRestPath();

}
