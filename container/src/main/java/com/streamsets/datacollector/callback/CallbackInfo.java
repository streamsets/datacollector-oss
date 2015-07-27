/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
        LOG.warn("Error while serializing slave metrics: , {}", ex.getMessage(), ex);
      }
    }
    return metricRegistryJson;
  }

  @Override
  public int compareTo(CallbackInfo o) {
    return sdcURL.compareToIgnoreCase(o.sdcURL);
  }
}
