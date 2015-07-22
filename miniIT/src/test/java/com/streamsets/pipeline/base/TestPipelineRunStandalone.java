/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.base;

import com.streamsets.datacollector.MiniSDC;
import com.streamsets.pipeline.MiniSDCTestingUtility;
import com.streamsets.pipeline.util.VerifyUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class TestPipelineRunStandalone {

  protected abstract String getPipelineJson() throws Exception;

  protected abstract int getRecordsInOrigin();

  protected abstract int getRecordsInTarget() throws IOException;

  private MiniSDCTestingUtility miniSDCTestingUtility;

  @Before
  public void setUp() throws Exception {
    miniSDCTestingUtility = new MiniSDCTestingUtility();
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testPipelineRun() throws Exception {
    System.setProperty("sdc.testing-mode", "true");
    try {
      MiniSDC miniSDC = miniSDCTestingUtility.createMiniSDC(MiniSDC.ExecutionMode.STANDALONE);
      miniSDC.startSDC();
      URI serverURI = miniSDC.getServerURI();
      miniSDC.createAndStartPipeline(getPipelineJson());
      //FIXME<Hari>: Do we need to wait for 5 seconds?
      Thread.sleep(5000);
      Map<String, Map<String, Integer>> countersMap = VerifyUtils.getMetrics(serverURI);
      int recordsInTarget = getRecordsInTarget();
      int recordsInOrigin = getRecordsInOrigin();

      assertEquals("Output records counter for source should be equal to " + recordsInOrigin, recordsInOrigin,
        VerifyUtils.getSourceOutputRecords(countersMap));
      assertEquals("Output records counter for target should be equal to " + recordsInTarget, recordsInTarget,
        VerifyUtils.getTargetOutputRecords(countersMap));
      assertEquals("Records output by Origin should be same as Records in Destination", recordsInOrigin,
        recordsInTarget);

    } finally {
      miniSDCTestingUtility.stopMiniSDC();
    }
  }

}
