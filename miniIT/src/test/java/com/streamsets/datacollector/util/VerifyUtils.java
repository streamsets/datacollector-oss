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
package com.streamsets.datacollector.util;

import org.glassfish.jersey.client.filter.CsrfProtectionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class VerifyUtils {
  private static final Logger LOG = LoggerFactory.getLogger(VerifyUtils.class);

  public static Map<String, Map<String, Object>> getCounters(List<URI> serverURIList, String name, String rev) throws Exception {
    Map<String, Map<String, Object>> countersMap = null;
    for (URI serverURI: serverURIList) {
      try {
        while (countersMap == null) {
          Thread.sleep(500);
          countersMap = getCountersFromMetrics(serverURI, name, rev);
        }
      } catch (IOException ioe) {
        LOG.warn("Failed while retrieving counters from " + serverURI);
      }
      if (countersMap!=null) {
        return countersMap;
      }
    }
    return null;
  }


  public static int getSourceOutputRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Source.*outputRecords.meter");
  }

  public static int getSourceErrorRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Source.*errorRecords.meter");
  }

  public static int getSourceInputRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Source.*inputRecords.meter");
  }

  public static int getSourceStageErrors(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Source.*stageErrors.meter");
  }

  public static int getTargetOutputRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Target.*outputRecords.meter");
  }

  public static int getTargetErrorRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Target.*errorRecords.meter");
  }

  public static int getTargetInputRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Target.*inputRecords.meter");
  }

  public static int getTargetStageErrors(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Target.*stageErrors.meter");
  }

  public static Map<String, Map<String, Object>> getCountersFromMetrics(URI serverURI, String name, String rev)
    throws IOException, InterruptedException {
    Map<String, Map<String, Map<String, Object>>> map = getMetrics(serverURI, name, rev);
    Map<String, Map<String, Object>> countersMap = map != null ? map.get("meters") : null;
    return countersMap;
  }

  public static Map<String, Map<String, Object>> getGaugesFromMetrics(URI serverURI, String name, String rev)
    throws IOException, InterruptedException {
    Map<String, Map<String, Map<String, Object>>> map = getMetrics(serverURI, name, rev);
    Map<String, Map<String, Object>> countersMap = map.get("gauges");
    return countersMap;
  }

  public static Map<String, Map<String, Map<String, Object>>> getMetrics(URI serverURI, String name, String rev)
    throws IOException, InterruptedException {
    LOG.info("Retrieving counters from Slave URI " + serverURI);
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/metrics")
      .queryParam("name", name).queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    Map<String, Map<String, Map<String, Object>>> map = response.readEntity(Map.class);
    return map;
  }

  public static void deleteAlert(URI serverURI, String name, String rev, String alertId) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/alerts").queryParam("rev", rev)
      .queryParam("alertId", alertId);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).delete();
    checkResponse(response, Response.Status.OK);
  }

  public static void startPipeline(URI serverURI, String name, String rev) throws IOException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/start").queryParam("rev", rev);

    Response response =
      target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.entity("", MediaType.APPLICATION_JSON));
    checkResponse(response, Response.Status.OK);
  }

  public static void stopPipeline(URI serverURI, String name, String rev) throws IOException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/stop").queryParam("rev", rev);
    Response response =
      target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.entity("", MediaType.APPLICATION_JSON));
    checkResponse(response, Response.Status.OK);
  }

  public static String getPipelineState(URI serverURI, String name, String rev) throws IOException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/status").queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    Map<String, String> map = response.readEntity(Map.class);
    return map.get("status");
  }

  public static void deleteHistory(URI serverURI, String name, String rev) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/history").queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).delete();
    checkResponse(response, Response.Status.OK);
  }

  public static List<Map<String, Object>> getHistory(URI serverURI, String name, String rev) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/history").queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    return response.readEntity(new GenericType<List<Map<String, Object>>>() {});
  }

  public static void captureSnapshot(URI serverURI, String name, String rev, String snapshotName, int batchSize)
    throws IOException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/snapshot/" + snapshotName)
      .queryParam("batchSize", batchSize).queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
      .put(Entity.entity("", MediaType.APPLICATION_JSON));
    checkResponse(response, Response.Status.OK);
  }

  public static boolean snapShotExists(URI serverURI, String name, String rev, String snapshotName) throws IOException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target =
      client.target(serverURI.toURL().toString())
        .path("/rest/v1/pipeline/" + name + "/snapshot/" + snapshotName + "/status").queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    Map<String, Object> map = response.readEntity(Map.class);
    return !(Boolean) map.get("inProgress");
  }

  public static Map<String, List<List<Map<String, Object>>>> getSnapShot(URI serverURI, String pipelineName, String rev,
                                                                   String snapshotName) throws MalformedURLException {
    ///snapshots/{pipelineName}/{snapshotName}
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + pipelineName
      + "/snapshot/" + snapshotName).queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    return response.readEntity(new GenericType<Map<String, List<List<Map<String, Object>>>>>() {});
  }

  public static String preview(URI serverURI, String pipelineName, String rev)
    throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + pipelineName
      + "/preview").queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.entity("", MediaType.APPLICATION_JSON));
    checkResponse(response, Response.Status.OK);
    Map<String, Object> map = response.readEntity(Map.class);
    System.out.println("Previewer id " + map.get("previewerId"));
    return (String)map.get("previewerId");
  }

  public static boolean isPreviewDone(URI serverURI, String previewerId) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/foo/preview/" + previewerId + "/" + "status");
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    Map<String, Object> map = response.readEntity(Map.class);
    System.out.println("Status is " + map.get("status"));
    String status = (String)map.get("status");
    return status.equals("FINISHED");
  }

  public static Map<String, Object> getPreviewOutput(URI serverURI, String previewerId) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    client.register(new CsrfProtectionFilter("CSRF"));
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/foo/preview/" + previewerId);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    System.out.println();
    return response.readEntity(Map.class);
  }

  public static void waitForPipelineToStop(URI serverURI, String name, String rev) throws InterruptedException, IOException {
    while(!"STOPPED".equals(getPipelineState(serverURI, name, rev))) {
      Thread.sleep(200);
    }
  }

  public static void waitForPipelineToStart(URI serverURI, String name, String rev) throws InterruptedException, IOException {
    while(!"RUNNING".equals(getPipelineState(serverURI, name, rev))) {
      Thread.sleep(200);
    }
  }

  public static void waitForSnapshot(URI serverURI, String name, String rev, String snapshotName) throws InterruptedException, IOException {
    while(!snapShotExists(serverURI, name, rev, snapshotName)) {
      Thread.sleep(500);
    }
  }

  public static void waitForPreview(URI serverURI, String previewerId) throws MalformedURLException,
    InterruptedException {
    while (!isPreviewDone(serverURI, previewerId)) {
      Thread.sleep(200);
    }
  }

  private static void checkResponse(Response response, Response.Status expectedStatus) {
    if(response.getStatusInfo().getStatusCode() != expectedStatus.getStatusCode()) {
      if (response.getStatusInfo().getStatusCode() == Response.Status.NO_CONTENT.getStatusCode()) {
        LOG.debug("Failed with No Content: 204, will retry again");
        return;
      }
      throw new RuntimeException("Request Failed with Error code : " + response.getStatusInfo().getStatusCode()
        + ". Error details: " + response.getStatusInfo().getReasonPhrase());
    }
  }

  private static int getCounter(Map<String, Map<String, Object>> metrics, String counterRegex) {
    String stage;
    int count = 0;
    for (Map.Entry<String, Map<String, Object>> mapEntry : metrics.entrySet()) {
      LOG.debug("Counter key:value " + mapEntry.getKey() + ":" + mapEntry.getValue().get("count"));
      if (mapEntry.getKey().matches(counterRegex)) {
        stage = mapEntry.getKey();
        count = (Integer) metrics.get(stage).get("count");
      }
    }
    return count;
  }

}

