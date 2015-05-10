/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline;

import static org.junit.Assert.assertEquals;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.junit.Test;

import com.google.common.io.Resources;
import com.streamsets.pipeline.MiniSDC.ExecutionMode;
import com.streamsets.pipeline.util.VerifyUtils;

public class TestSimpleMiniSDC {

  @Test
  public void testSimpleSDC() throws Exception {
    System.setProperty("sdc.testing-mode", "true");
    URI uri = Resources.getResource("simple_pipeline.json").toURI();
    int expectedRecords = 500; // in the pipeline file
    String pipelineJson = new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    MiniSDCTestingUtility miniSDCTestingUtility = new MiniSDCTestingUtility();
    try {
      MiniSDC miniSDC = miniSDCTestingUtility.startMiniSDC(pipelineJson, ExecutionMode.STANDALONE);
      URI serverURI = miniSDC.getServerURI();
      Thread.sleep(10000);
      Map<String, Map<String, Integer>> countersMap = VerifyUtils.getCounters(serverURI);
      assertEquals("Output records counter for source should be equal to" + expectedRecords, expectedRecords,
        VerifyUtils.getSourceCounters(countersMap));
      assertEquals("Output records counter for target should be equal to" + expectedRecords, expectedRecords,
        VerifyUtils.getTargetCounters(countersMap));
    } finally {
      miniSDCTestingUtility.stopMiniSDC();
    }
  }

}
