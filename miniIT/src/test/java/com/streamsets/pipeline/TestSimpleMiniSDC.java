/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import com.google.common.io.Resources;
import com.streamsets.pipeline.MiniSDC.ExecutionMode;
import com.streamsets.pipeline.util.VerifyUtils;
import org.junit.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestSimpleMiniSDC {

  @Test
  public void testSimpleSDC() throws Exception {
    System.setProperty("sdc.testing-mode", "true");
    URI uri = Resources.getResource("simple_pipeline.json").toURI();
    int expectedRecords = 500; // in the pipeline file
    String pipelineJson = new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    MiniSDCTestingUtility miniSDCTestingUtility = new MiniSDCTestingUtility();
    try {
      MiniSDC miniSDC = miniSDCTestingUtility.createMiniSDC(ExecutionMode.STANDALONE);
      miniSDC.startSDC();
      miniSDC.createAndStartPipeline(pipelineJson);
      URI serverURI = miniSDC.getServerURI();
      Thread.sleep(10000);
      Map<String, Map<String, Integer>> countersMap = VerifyUtils.getMetrics(serverURI);
      assertEquals("Output records counter for source should be equal to" + expectedRecords, expectedRecords,
        VerifyUtils.getSourceOutputRecords(countersMap));
      assertEquals("Output records counter for target should be equal to" + expectedRecords, expectedRecords,
        VerifyUtils.getTargetOutputRecords(countersMap));
    } finally {
      miniSDCTestingUtility.stopMiniSDC();
    }
  }

}
