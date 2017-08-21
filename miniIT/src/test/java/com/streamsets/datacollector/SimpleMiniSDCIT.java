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
package com.streamsets.datacollector;

import com.google.common.io.Resources;
import com.streamsets.datacollector.MiniSDC;
import com.streamsets.datacollector.MiniSDC.ExecutionMode;
import com.streamsets.datacollector.util.VerifyUtils;

import org.junit.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SimpleMiniSDCIT {

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
      Map<String, Map<String, Object>> countersMap = VerifyUtils.getCountersFromMetrics(serverURI, "admin", "0");
      assertEquals("Output records counter for source should be equal to " + expectedRecords, expectedRecords,
        VerifyUtils.getSourceOutputRecords(countersMap));
      assertEquals("Output records counter for target should be equal to " + expectedRecords, expectedRecords,
        VerifyUtils.getTargetOutputRecords(countersMap));
    } finally {
      miniSDCTestingUtility.stopMiniSDC();
    }
  }

}
