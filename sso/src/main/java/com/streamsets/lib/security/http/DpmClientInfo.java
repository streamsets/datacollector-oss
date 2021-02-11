/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.lib.security.http;

import java.util.Map;

/**
 * Interface that provides access to DPM client configuration. To obtain URL and credentials to contact DPM and
 * to update the DPM URL in case the user detects a change.
 */
public interface DpmClientInfo {

  /**
   * Using this constant, the client info can be retrieved from the RuntimeInfo singleton.
   */
  String RUNTIME_INFO_ATTRIBUTE_KEY = DpmClientInfo.class.getSimpleName();

  /**
   * Returns DPM base URL.
   */
  String getDpmBaseUrl();

  /**
   * Returns the headers (with credentials) to contact DPM.
   */
  Map<String, String> getHeaders();

  /**
   * Sets the new DPM base URL, the change should be persistent.
   */
  void setDpmBaseUrl(String dpmBaseUrl);

}
