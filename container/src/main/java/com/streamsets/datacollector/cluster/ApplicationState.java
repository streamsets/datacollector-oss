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


import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;

public class ApplicationState {
  private static final String ID = "id";
  private static final String dirID = "dirId";
  private static final String SDC_TOKEN = "sdcToken";
  private final Map<String, Object> backingMap;

  public ApplicationState() {
    this.backingMap = new HashMap<>();
  }
  public ApplicationState(Map<String, Object> map) {
    this();
    if (map != null) {
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        if (entry.getValue() != null) {
          this.backingMap.put(entry.getKey(), entry.getValue());
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

  public Optional<String> getDirId() {
    return Optional.fromNullable((String)backingMap.get(dirID));
  }

  public String getId() {
    return (String) backingMap.get(ID);
  }

  public void setId(String id) {
    if (id != null) {
      this.backingMap.put(ID, id);
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
