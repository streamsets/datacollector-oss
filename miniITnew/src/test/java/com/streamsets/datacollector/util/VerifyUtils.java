/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.util;

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
    return getCounter(metrics, "stage.*Source.*outputRecords.counter");
  }

  public static int getSourceErrorRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Source.*errorRecords.counter");
  }

  public static int getSourceInputRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Source.*inputRecords.counter");
  }

  public static int getSourceStageErrors(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Source.*stageErrors.counter");
  }

  public static int getTargetOutputRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Target.*outputRecords.counter");
  }

  public static int getTargetErrorRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Target.*errorRecords.counter");
  }

  public static int getTargetInputRecords(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Target.*inputRecords.counter");
  }

  public static int getTargetStageErrors(Map<String, Map<String, Object>> metrics) {
    return getCounter(metrics, "stage.*Target.*stageErrors.counter");
  }

  public static Map<String, Map<String, Object>> getCountersFromMetrics(URI serverURI, String name, String rev)
    throws IOException, InterruptedException {
    Map<String, Map<String, Map<String, Object>>> map = getMetrics(serverURI, name, rev);
    Map<String, Map<String, Object>> countersMap = map != null ? map.get("counters") : null;
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
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/metrics")
      .queryParam("name", name).queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    if (response.getStatusInfo().equals(Response.noContent().build().getStatusInfo())) {
      LOG.debug("Got empty status info");
      return null;
    }
    checkResponse(response, Response.Status.OK);
    Map<String, Map<String, Map<String, Object>>> map = response.readEntity(Map.class);
    return map;
  }

  public static void deleteAlert(URI serverURI, String name, String rev, String alertId) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/alerts").queryParam("rev", rev)
      .queryParam("alertId", alertId);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).delete();
    checkResponse(response, Response.Status.OK);
  }

  public static void startPipeline(URI serverURI, String name, String rev) throws IOException {
    Client client = ClientBuilder.newClient();
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/start").queryParam("rev", rev);

    Response response =
      target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.entity("", MediaType.APPLICATION_JSON));
    checkResponse(response, Response.Status.OK);
  }

  public static void stopPipeline(URI serverURI, String name, String rev) throws IOException {
    Client client = ClientBuilder.newClient();
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/stop").queryParam("rev", rev);
    Response response =
      target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.entity("", MediaType.APPLICATION_JSON));
    checkResponse(response, Response.Status.OK);
  }

  public static String getPipelineState(URI serverURI, String name, String rev) throws IOException {
    Client client = ClientBuilder.newClient();
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/status").queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    Map<String, String> map = response.readEntity(Map.class);
    return map.get("status");
  }

  public static void deleteHistory(URI serverURI, String name, String rev) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/history").queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).delete();
    checkResponse(response, Response.Status.OK);
  }

  public static List<Map<String, Object>> getHistory(URI serverURI, String name, String rev) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/history").queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    return response.readEntity(new GenericType<List<Map<String, Object>>>() {});
  }

  public static void captureSnapshot(URI serverURI, String name, String rev, String snapshotName, int batchSize)
    throws IOException {
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + name + "/snapshot/" + snapshotName)
      .queryParam("batchSize", batchSize).queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
      .put(Entity.entity("", MediaType.APPLICATION_JSON));
    checkResponse(response, Response.Status.OK);
  }

  public static boolean snapShotExists(URI serverURI, String name, String rev, String snapshotName) throws IOException {
    Client client = ClientBuilder.newClient();
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
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/pipeline/" + pipelineName
      + "/snapshot/" + snapshotName).queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    return response.readEntity(new GenericType<Map<String, List<List<Map<String, Object>>>>>() {});
  }

  public static String preview(URI serverURI, String pipelineName, String rev)
    throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/preview/" + pipelineName
      + "/" + "create").queryParam("rev", rev);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.entity("", MediaType.APPLICATION_JSON));
    checkResponse(response, Response.Status.OK);
    Map<String, Object> map = response.readEntity(Map.class);
    System.out.println("Previewer id " + map.get("previewerId"));
    return (String)map.get("previewerId");
  }

  public static boolean isPreviewDone(URI serverURI, String previewerId) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    WebTarget target =
      client.target(serverURI.toURL().toString()).path("/rest/v1/preview-id/" + previewerId + "/" + "status");
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    Map<String, Object> map = response.readEntity(Map.class);
    System.out.println("Status is " + map.get("status"));
    String status = (String)map.get("status");
    return status.equals("FINISHED");
  }

  public static Map<String, Object> getPreviewOutput(URI serverURI, String previewerId) throws MalformedURLException {
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(serverURI.toURL().toString()).path("/rest/v1/preview-id/" + previewerId);
    Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    checkResponse(response, Response.Status.OK);
    System.out.println();
    return response.readEntity(Map.class);
  }

  public static void waitForPipelineToStop(URI serverURI, String name, String rev) throws InterruptedException, IOException {
    while(!"STOPPED".equals(getPipelineState(serverURI, name, rev))) {
      Thread.sleep(500);
    }
  }

  public static void waitForPipelineToStart(URI serverURI, String name, String rev) throws InterruptedException, IOException {
    while(!"RUNNING".equals(getPipelineState(serverURI, name, rev))) {
      Thread.sleep(500);
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
      Thread.sleep(500);
    }
  }

  private static void checkResponse(Response response, Response.Status expectedStatus) {
    if(response.getStatusInfo().getStatusCode() != expectedStatus.getStatusCode()) {
      throw new RuntimeException("Request Failed with Error code : " + response.getStatusInfo()
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

