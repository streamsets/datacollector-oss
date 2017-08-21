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
package com.streamsets.datacollector.base;

import com.streamsets.datacollector.util.VerifyUtils;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@FixMethodOrder
public abstract class PipelineOperationsBaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineOperationsBaseIT.class);

  protected abstract URI getServerURI();

  protected abstract List<URI> getWorkerURI() throws URISyntaxException;

  protected abstract String getPipelineName();

  protected abstract String getPipelineRev();

  protected abstract boolean clusterModeTest();

  @Test(timeout = 120000)
  public void testCaptureSnapshot() throws Exception {
    String status = VerifyUtils.getPipelineState(getServerURI(), getPipelineName(), getPipelineRev());
    while (!("RUNNING".equals(status))) {
      Thread.sleep(200);
      status = VerifyUtils.getPipelineState(getServerURI(), getPipelineName(), getPipelineRev());
    }
    if(clusterModeTest()) {
      Assert.assertTrue(getWorkerURI().size() > 0);
      for(URI workerURI : getWorkerURI()) {
        testCaptureSnapshot(workerURI);
      }
    } else {
      testCaptureSnapshot(getServerURI());
    }
  }

  @Test(timeout = 120000)
  public void testMetrics() throws Exception {
    URI serverURI = getServerURI();
    String status = VerifyUtils.getPipelineState(serverURI, getPipelineName(), getPipelineRev());
    while (!("RUNNING".equals(status))) {
      Thread.sleep(200);
      status = VerifyUtils.getPipelineState(serverURI, getPipelineName(), getPipelineRev());
    }
    testMetrics(serverURI);
    if(clusterModeTest()) {
      Assert.assertTrue(getWorkerURI().size() > 0);
      for (URI workerURI : getWorkerURI()) {
        testMetrics(workerURI);
      }
    }
  }

  private void testCaptureSnapshot(URI serverURI) throws Exception {
    String status = VerifyUtils.getPipelineState(serverURI, getPipelineName(), getPipelineRev());
    while (!("RUNNING".equals(status))) {
      Thread.sleep(200);
      status = VerifyUtils.getPipelineState(serverURI, getPipelineName(), getPipelineRev());
    }
    final String snapshotName = UUID.randomUUID().toString();

    VerifyUtils.captureSnapshot(serverURI, getPipelineName(), getPipelineRev(), snapshotName, 10);
    VerifyUtils.waitForSnapshot(serverURI, getPipelineName(), getPipelineRev(), snapshotName);

    Map<String, List<List<Map<String, Object>>>> snapShot =
      VerifyUtils.getSnapShot(serverURI, getPipelineName(), getPipelineRev(), snapshotName);
    List<Map<String, Object>> stageOutputs = snapShot.get("snapshotBatches").get(0);
    List<Map<String, Object>> records = getRecords(stageOutputs);
    while (records == null) {
      Thread.sleep(500);
      LOG.debug("Got empty records from stageOutput of snaphot, retrying again");
      snapShot = VerifyUtils.getSnapShot(serverURI, getPipelineName(), getPipelineRev(), snapshotName);
      stageOutputs = snapShot.get("snapshotBatches").get(0);
      records = getRecords(stageOutputs);
    }
    for (Map<String, Object> record : records) {
      // each record has header and value
      Map<String, Object> val = (Map<String, Object>) record.get("value");
      Assert.assertNotNull(val);
      // value has root field with path "", and Map with key "text" for the text field
      Assert.assertTrue(val.containsKey("value"));
      Map<String, Map<String, String>> value = (Map<String, Map<String, String>>) val.get("value");
      Assert.assertNotNull(value);
      // The text field in the record [/text]
      if (value.containsKey("text")) {
        // Kafka origin pipelines generate record with text data.
        // Additional tests for those
        Map<String, String> text = value.get("text");
        Assert.assertNotNull(text);
        // Field has type, path and value
        Assert.assertTrue(text.containsKey("value"));
        Assert.assertEquals("Hello Kafka", text.get("value"));
        Assert.assertTrue(text.containsKey("sqpath"));
        Assert.assertEquals("/text", text.get("sqpath"));
        Assert.assertTrue(text.containsKey("type"));
        Assert.assertEquals("STRING", text.get("type"));
      }
    }

  }

  private List<Map<String, Object>> getRecords(List<Map<String, Object>> stageOutputs) {
    Assert.assertNotNull(stageOutputs);
    for (Map<String, Object> stageOutput : stageOutputs) {
      LOG.info("stageOutput = " + stageOutput.keySet());
      Map<String, Object> output = (Map<String, Object>) stageOutput.get("output");
      for (Map.Entry<String, Object> e : output.entrySet()) {
        LOG.info("output key = " + e.getKey());
        Assert.assertTrue(e.getValue() instanceof List);
        List<Map<String, Object>> records = (List<Map<String, Object>>) e.getValue();
        return records;
      }
    }
    return null;

  }

  private void testMetrics(URI serverURI) throws IOException, InterruptedException {
    String status = VerifyUtils.getPipelineState(serverURI, getPipelineName(), getPipelineRev());
    while (!("RUNNING".equals(status))) {
      Thread.sleep(200);
      status = VerifyUtils.getPipelineState(serverURI, getPipelineName(), getPipelineRev());
    }
    Thread.sleep(2000);
    Map<String, Map<String, Object>> metrics = VerifyUtils.getCountersFromMetrics(serverURI, getPipelineName(), getPipelineRev());

    while (VerifyUtils.getSourceOutputRecords(metrics) == 0) {
      metrics = VerifyUtils.getCountersFromMetrics(serverURI, getPipelineName(), getPipelineRev());
      LOG.debug("Got 0 output records from source, retrying");
      Thread.sleep(200);
    }

    Assert.assertTrue(VerifyUtils.getSourceOutputRecords(metrics) > 0);
    Assert.assertTrue(VerifyUtils.getSourceInputRecords(metrics) == 0);
    Assert.assertTrue(VerifyUtils.getSourceErrorRecords(metrics) == 0);
    Assert.assertTrue(VerifyUtils.getSourceStageErrors(metrics) == 0);

    Assert.assertTrue(VerifyUtils.getTargetOutputRecords(metrics) > 0);
    Assert.assertTrue(VerifyUtils.getTargetInputRecords(metrics) > 0);
    Assert.assertTrue(VerifyUtils.getTargetErrorRecords(metrics) == 0);
    Assert.assertTrue(VerifyUtils.getTargetStageErrors(metrics) == 0);
  }

}
