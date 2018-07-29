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
package com.streamsets.datacollector.cluster;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ApplicationState {
  private static final String APP_ID = "id";
  private static final String dirID = "dirId";
  private static final String SDC_TOKEN = "sdcToken";
  private static final String EMR_CONFIG = "emrConfig";
  private final Map<String, Object> backingMap;

  public ApplicationState() {
    this.backingMap = new HashMap<>();
  }
  public ApplicationState(Map<String, Object> map) {
    this();
    if (map != null) {
      for (Map.Entry<String, Object> entry : map.entrySet()) {

        if (entry.getValue() != null) {
          if (entry.getKey().equals(EMR_CONFIG)) {
            Properties properties = new ObjectMapper().convertValue(entry.getValue(), Properties.class);
            this.backingMap.put(entry.getKey(), properties);
          } else {
            this.backingMap.put(entry.getKey(), entry.getValue());
          }
        }
      }
    }
  }

  public String getSdcToken() {
    return (String)backingMap.get(SDC_TOKEN);
  }

  public void setSdcToken(String sdcToken) {
    if (sdcToken != null) {
      this.backingMap.put(SDC_TOKEN, sdcToken);
    }
  }

  public void setDirId(String dirId) {
    if (dirId != null) {
      this.backingMap.put(dirID, dirId);
    }
  }

  public Properties getEmrConfig() {
    return (Properties) backingMap.get(EMR_CONFIG);
  }

  public void setEmrConfig(Properties properties) {
    this.backingMap.put(EMR_CONFIG, properties);
  }

  public Optional<String> getDirId() {
    return Optional.fromNullable((String)backingMap.get(dirID));
  }

  public String getAppId() {
    return (String) backingMap.get(APP_ID);
  }

  public void setAppId(String appId) {
    if (appId != null) {
      this.backingMap.put(APP_ID, appId);
    }
  }

  public Map<String, Object> getMap() {
    return ImmutableMap.copyOf(backingMap);
  }

  @Override
  public String toString() {
    return Utils.format("ApplicationState = {}", backingMap);
  }
}
