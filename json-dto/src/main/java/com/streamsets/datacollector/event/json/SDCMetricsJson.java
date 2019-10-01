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
package com.streamsets.datacollector.event.json;

import java.util.Map;

public class SDCMetricsJson {
  private long timestamp;
  private Map<String, String> metadata;
  private String sdcId;
  private MetricRegistryJson metrics;
  private boolean isAggregated;
  private String masterSdcId;

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public String getSdcId() {
    return sdcId;
  }

  public void setSdcId(String sdcId) {
    this.sdcId = sdcId;
  }

  public void setMasterSdcId(String masterSdcId) {
    this.masterSdcId = masterSdcId;
  }

  public String getMasterSdcId() {
    return masterSdcId;
  }

  public MetricRegistryJson getMetrics() {
    return metrics;
  }

  public void setMetrics(MetricRegistryJson metrics) {
    this.metrics = metrics;
  }

  public boolean isAggregated() {
    return isAggregated;
  }

  public void setAggregated(boolean aggregated) {
    isAggregated = aggregated;
  }
}
