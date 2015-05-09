/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.callback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.restapi.bean.MetricRegistryJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CallbackInfo {
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

  public CallbackInfo(String sdcClusterToken, String sdcSlaveToken, String sdcURL, String adminToken, String creatorToken,
                      String managerToken, String guestToken, String metrics) {
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
}
