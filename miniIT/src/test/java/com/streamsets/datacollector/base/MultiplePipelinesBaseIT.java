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

import com.streamsets.datacollector.MiniSDC;
import com.streamsets.datacollector.MiniSDCTestingUtility;
import com.streamsets.datacollector.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public abstract class MultiplePipelinesBaseIT {

  private static MiniSDCTestingUtility miniSDCTestingUtility;
  private static URI serverURI;
  private static MiniSDC miniSDC;

  protected abstract Map<String, String> getPipelineNameAndRev();

  public static void beforeClass(List<String> pipelineJsonList) throws Exception {
    System.setProperty("sdc.testing-mode", "true");
    miniSDCTestingUtility = new MiniSDCTestingUtility();
    miniSDC = miniSDCTestingUtility.createMiniSDC(MiniSDC.ExecutionMode.STANDALONE);
    miniSDC.startSDC();
    serverURI = miniSDC.getServerURI();
    for (String pipelineJson : pipelineJsonList) {
      miniSDC.createPipeline(pipelineJson);
    }
  }

  /**
   * The extending test must call this method in the method scheduled to run after class
   * @throws Exception
   */
  public static void afterClass() throws Exception {
    miniSDCTestingUtility.stopMiniSDC();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    //sequential pipeline starts
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.startPipeline(serverURI, e.getKey(), e.getValue());
      VerifyUtils.waitForPipelineToStart(serverURI, e.getKey(), e.getValue());
    }
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    //stops
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.stopPipeline(serverURI, e.getKey(), e.getValue());
      VerifyUtils.waitForPipelineToStop(serverURI, e.getKey(), e.getValue());
    }
  }

  /**********************************************************/
  /************************* tests **************************/
  /**********************************************************/

  //The following tests can be bumped up to the Base class to be available for cluster mode once we have a way to
  // detect that the pipeline is running in the worker nodes.
  //As of now even though the state says RUNNING the worker nodes may not have started processing the data.

  @Test
  public void testRestartAndHistory() throws Exception {

    //Almost-simultaneous stops
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.stopPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStop(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("STOPPED", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    //clear history
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.deleteHistory(serverURI, e.getKey(), e.getValue());
      List<Map<String, Object>> history = VerifyUtils.getHistory(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals(0, history.size());
    }

    //fresh almost-simultaneous starts
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.startPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStart(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    //Almost-simultaneous stops
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.stopPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStop(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("STOPPED", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    //fresh almost-simultaneous starts
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.startPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStart(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    //Almost-simultaneous stops
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.stopPipeline(serverURI, e.getKey(), e.getValue());
    }
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.waitForPipelineToStop(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals("STOPPED", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
    }

    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      List<Map<String, Object>> history = VerifyUtils.getHistory(serverURI, e.getKey(), e.getValue());
      Assert.assertEquals(8, history.size());
    }

    //sequential pipeline starts
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      VerifyUtils.startPipeline(serverURI, e.getKey(), e.getValue());
      VerifyUtils.waitForPipelineToStart(serverURI, e.getKey(), e.getValue());
    }
  }

  @Test
  public void testCaptureSnapshot() throws Exception {
    for(Map.Entry<String, String> nameAndRev : getPipelineNameAndRev().entrySet()) {
      String snapShotName = "snapShot_" + nameAndRev.getKey() + "_" + nameAndRev.getValue();

      VerifyUtils.captureSnapshot(serverURI, nameAndRev.getKey(), nameAndRev.getValue(), snapShotName, 1);
      VerifyUtils.waitForSnapshot(serverURI, nameAndRev.getKey(), nameAndRev.getValue(), snapShotName);

      Map<String, List<List<Map<String, Object>>>> snapShot = VerifyUtils.getSnapShot(serverURI, nameAndRev.getKey(),
        nameAndRev.getValue(), snapShotName);

      List<Map<String, Object>> stageOutputs = snapShot.get("snapshotBatches").get(0);
      Assert.assertNotNull(stageOutputs);

      for (Map<String, Object> stageOutput : stageOutputs) {
        Map<String, Object> output = (Map<String, Object>) stageOutput.get("output");
        for (Map.Entry<String, Object> e : output.entrySet()) {
          Assert.assertTrue(e.getValue() instanceof List);
          List<Map<String, Object>> records = (List<Map<String, Object>>) e.getValue();
          //This is the list of records
          for (Map<String, Object> record : records) {
            //each record has header and value
            Map<String, Object> val = (Map<String, Object>) record.get("value");
            Assert.assertNotNull(val);
          }
        }
      }
    }
  }

  @Test()
  public void testMetrics() throws Exception {
    for(Map.Entry<String, String> e : getPipelineNameAndRev().entrySet()) {
      Assert.assertEquals("RUNNING", VerifyUtils.getPipelineState(serverURI, e.getKey(), e.getValue()));
      Thread.sleep(2000);
      Map<String, Map<String, Object>> metrics = VerifyUtils.getCountersFromMetrics(serverURI, e.getKey(), e.getValue());

      Assert.assertTrue(VerifyUtils.getSourceOutputRecords(metrics) > 0);
      Assert.assertTrue(VerifyUtils.getSourceInputRecords(metrics) == 0);
      Assert.assertTrue(VerifyUtils.getSourceErrorRecords(metrics) == 0);
      Assert.assertTrue(VerifyUtils.getSourceStageErrors(metrics) == 0);

      Assert.assertTrue("No target output records for pipeline " + e.getKey(), VerifyUtils.getTargetOutputRecords(metrics) > 0);
      Assert.assertTrue(VerifyUtils.getTargetInputRecords(metrics) > 0);
      Assert.assertTrue(VerifyUtils.getTargetErrorRecords(metrics) == 0);
      Assert.assertTrue(VerifyUtils.getTargetStageErrors(metrics) == 0);
    }
  }

}
