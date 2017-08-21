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
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class PipelineRunStandaloneIT {

  protected abstract String getPipelineJson() throws Exception;

  protected abstract int getRecordsInOrigin();

  protected abstract int getRecordsInTarget() throws IOException;

  protected abstract String getPipelineName();

  protected abstract String getPipelineRev();

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
      Map<String, Map<String, Object>> countersMap = VerifyUtils.getCountersFromMetrics(serverURI, getPipelineName(),
        getPipelineRev());
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

  private static void checkResponse(Response response, Response.Status expectedStatus) {
    if(response.getStatusInfo().getStatusCode() != expectedStatus.getStatusCode()) {
      throw new RuntimeException("Request Failed with Error code : " + response.getStatusInfo()
        + ". Error details: " + response.getStatusInfo().getReasonPhrase());
    }
  }

}
