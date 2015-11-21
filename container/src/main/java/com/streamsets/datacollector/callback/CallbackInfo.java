/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.callback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CallbackInfo implements Comparable<CallbackInfo> {
  private final static Logger LOG = LoggerFactory.getLogger(CallbackInfo.class);
  private final String sdcClusterToken;
  private final String sdcSlaveToken;
  private final String sdcURL;
  private final String adminToken;
  private final String creatorToken;
  private final String managerToken;
  private final String guestToken;
  private final String metrics;
  private MetricRegistryJson metricRegistryJson;
  private final String name;
  private final String rev;
  private final String user;

  public CallbackInfo(String user, String name, String rev, String sdcClusterToken, String sdcSlaveToken,
                      String sdcURL, String adminToken, String creatorToken, String managerToken, String guestToken, String metrics) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.sdcClusterToken = sdcClusterToken;
    this.sdcSlaveToken = sdcSlaveToken;
    this.sdcURL = sdcURL;
    this.adminToken = adminToken;
    this.creatorToken = creatorToken;
    this.managerToken = managerToken;
    this.guestToken = guestToken;
    this.metrics = metrics;
  }

  public String getSdcClusterToken() {
    return sdcClusterToken;
  }

  public String getSdcSlaveToken() {
    return sdcSlaveToken;
  }

  public String getSdcURL() {
    return sdcURL;
  }

  public String getAdminToken() {
    return adminToken;
  }

  public String getManagerToken() {
    return managerToken;
  }

  public String getGuestToken() {
    return guestToken;
  }

  public String getCreatorToken() {
    return creatorToken;
  }

  public String getMetrics() {
    return metrics;
  }

  public String getName() {
    return name;
  }

  public String getUser() {
    return user;
  }

  public String getRev() {
    return rev;
  }

  public MetricRegistryJson getMetricRegistryJson() {
    if(metricRegistryJson == null) {
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      try {
        metricRegistryJson = objectMapper.readValue(this.metrics, MetricRegistryJson.class);
      } catch (IOException ex) {
        LOG.warn("Error while serializing slave metrics: , {}", ex.toString(), ex);
      }
    }
    return metricRegistryJson;
  }

  @Override
  public int compareTo(CallbackInfo o) {
    return sdcURL.compareToIgnoreCase(o.sdcURL);
  }

  public boolean equals(Object o) {
    return compareTo((CallbackInfo)o) == 0;
  }
}
