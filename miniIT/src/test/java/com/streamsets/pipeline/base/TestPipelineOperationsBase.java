/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.base;

import com.streamsets.pipeline.util.VerifyUtils;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

@FixMethodOrder
public abstract class TestPipelineOperationsBase {

  protected abstract URI getServerURI();

  protected abstract List<URI> getWorkerURI() throws URISyntaxException;

  protected abstract String getPipelineName();

  protected abstract String getPipelineRev();

  protected abstract boolean clusterModeTest();

  @Test
  public void testCaptureSnapshot() throws Exception {
    Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(getServerURI()));
    if(clusterModeTest()) {
      Assert.assertTrue(getWorkerURI().size() > 0);
      for(URI workerURI : getWorkerURI()) {
        testCaptureSnapshot(workerURI);
      }
    } else {
      testCaptureSnapshot(getServerURI());
    }
  }

  @Test()
  public void testMetrics() throws Exception {
    URI serverURI = getServerURI();
    Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(serverURI));
    testMetrics(serverURI);
    if(clusterModeTest()) {
      Assert.assertTrue(getWorkerURI().size() > 0);
      for (URI workerURI : getWorkerURI()) {
        testMetrics(workerURI);
      }
    }
  }

  private void testCaptureSnapshot(URI serverURI) throws Exception {
    Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(serverURI));
    String snapShotName = "mySnapShot";

    VerifyUtils.captureSnapshot(serverURI, snapShotName, 10);
    VerifyUtils.waitForSnapshot(serverURI, snapShotName);

    Map<String, List<Map<String, Object>>> snapShot = VerifyUtils.getSnapShot(serverURI, getPipelineName(), getPipelineRev(), snapShotName);
    List<Map<String, Object>> stageOutputs = snapShot.get("snapshot");
    Assert.assertNotNull(stageOutputs);

    for (Map<String, Object> stageOutput : stageOutputs) {
      Map<String, Object> output = (Map<String, Object>) stageOutput.get("output");
      for (Map.Entry<String, Object> e : output.entrySet()) {
        Assert.assertTrue(e.getValue() instanceof List);
        Assert.assertTrue(((List<?>) e.getValue()).size() > 0);
        //This is the list of records
        List<Map<String, Object>> records = (List<Map<String, Object>>) e.getValue();
        for (Map<String, Object> record : records) {
          //each record has header and value
          Map<String, Object> val = (Map<String, Object>) record.get("value");
          Assert.assertNotNull(val);
          //value has root field with path "", and Map with key "text" for the text field
          Assert.assertTrue(val.containsKey("value"));
          Map<String, Map<String, String>> value = (Map<String, Map<String, String>>) val.get("value");
          Assert.assertNotNull(value);
          //The text field in the record [/text]
          if(value.containsKey("text")) {
            //Kafka origin pipelines generate record with text data.
            //Additional tests for those
            Map<String, String> text = value.get("text");
            Assert.assertNotNull(text);
            //Field has type, path and value
            Assert.assertTrue(text.containsKey("value"));
            Assert.assertEquals("Hello Kafka", text.get("value"));
            Assert.assertTrue(text.containsKey("path"));
            Assert.assertEquals("/text", text.get("path"));
            Assert.assertTrue(text.containsKey("type"));
            Assert.assertEquals("STRING", text.get("type"));
          }
        }
      }
    }
  }

  private void testMetrics(URI serverURI) throws IOException, InterruptedException {
    Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(serverURI));
    Thread.sleep(2000);
    Map<String, Map<String, Integer>> metrics = VerifyUtils.getMetrics(serverURI);

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
