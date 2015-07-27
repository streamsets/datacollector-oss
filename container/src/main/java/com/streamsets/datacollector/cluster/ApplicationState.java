/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.cluster;


import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.HashMap;
import java.util.Map;

public class ApplicationState {
  private static final String ID = "id";
  private static final String SDC_TOKEN = "sdcToken";
  private Map<String, Object> backingMap;

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

  public String getId() {
    return (String)backingMap.get(ID);
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
