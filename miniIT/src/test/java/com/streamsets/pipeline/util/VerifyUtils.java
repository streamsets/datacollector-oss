/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class VerifyUtils {
  private static final Logger LOG = LoggerFactory.getLogger(VerifyUtils.class);

  public static Map<String, Map<String, Integer>> getCounters(List<URI> serverURIList) throws Exception {
    Map<String, Map<String, Integer>> countersMap = null;
    for (URI serverURI: serverURIList) {
      try {
        countersMap = getCounters(serverURI);
      } catch (IOException ioe) {
        LOG.warn("Failed while retrieving counters from " + serverURI);
      }
      if (countersMap!=null) {
        return countersMap;
      }
    }
    return null;
  }

  public static Map<String, Map<String, Integer>> getCounters(URI serverURI) throws IOException {
    // TODO - Get aggregated slave counters
    LOG.info("Retrieving counters from Slave URI " + serverURI);
    URL url = new URL(serverURI.getScheme(), serverURI.getHost(), serverURI.getPort(), "/rest/v1/pipeline/metrics");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");
    if (conn.getResponseCode() != 200) {
      BufferedReader responseBuffer = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = responseBuffer.readLine()) != null) {
        sb.append(line + "\n");
      }
      throw new RuntimeException("HTTP GET Request Failed with Error code : " + conn.getResponseCode()
        + "Error details: " + sb.toString());

    }
    BufferedReader responseBuffer = new BufferedReader(new InputStreamReader((conn.getInputStream())));
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = responseBuffer.readLine()) != null) {
      sb.append(line + "\n");
    }
    responseBuffer.close();
    ObjectMapper json = new ObjectMapper();
    Map<String, Map<String, Map<String, Integer>>> map = json.readValue(sb.toString(), Map.class);
    Map<String, Map<String, Integer>> countersMap = map.get("counters");
    conn.disconnect();
    return countersMap;
  }

  public static int getSourceCounters(Map<String, Map<String, Integer>> countersMap) {
    String source = null;
    int count = 0;
    for (Map.Entry<String, Map<String, Integer>> mapEntry : countersMap.entrySet()) {
      LOG.debug("Counter key:value " + mapEntry.getKey() + ":" + mapEntry.getValue().get("count"));
      // TODO - Bring in some classes from container to parse json file
      if (mapEntry.getKey().matches("stage.*Source.*outputRecords.counter")) {
        source = mapEntry.getKey();
        count = countersMap.get(source).get("count");
      }
    }
    return count;
  }

  public static int getTargetCounters(Map<String, Map<String, Integer>> countersMap) {
    String target = null;
    int count = 0;
    for (Map.Entry<String, Map<String, Integer>> mapEntry : countersMap.entrySet()) {
      LOG.debug("Counter key:value " + mapEntry.getKey() + ":" + mapEntry.getValue().get("count"));
      // TODO - Bring in some classes from container to parse json file
      if (mapEntry.getKey().matches("stage.*Target.*outputRecords.counter")) {
        target = mapEntry.getKey();
        count = countersMap.get(target).get("count");
      }
    }
    return count;
  }
}

