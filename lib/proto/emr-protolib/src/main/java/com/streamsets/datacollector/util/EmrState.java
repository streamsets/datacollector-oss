/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.util;

import java.util.Properties;

public class EmrState {

  private static final String STATE = "state";
  private static final String MESSAGE = "message";
  private static final String APP_ID = "appId";
  private Properties properties = new Properties();

  public void setState(String statusString) {
    if (statusString != null) {
      properties.put(STATE, statusString);
    }
  }

  public void setMessage(String message) {
    if (message != null) {
      properties.put(MESSAGE, message);
    }
  }

  public Properties toProperties() {
    return properties;
  }

  public void setAppId(String appId) {
    if (appId != null) {
      properties.put(APP_ID, appId);
    }
  }
}
