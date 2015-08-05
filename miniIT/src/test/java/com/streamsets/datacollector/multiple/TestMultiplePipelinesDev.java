/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.multiple;

import com.google.common.io.Resources;
import com.streamsets.datacollector.base.TestMultiplePipelinesBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestMultiplePipelinesDev extends TestMultiplePipelinesBase {

  private static List<String> getPipelineJson() throws URISyntaxException, IOException {
    //random to kafka
    URI uri = Resources.getResource("dev_multiple_pipelines.json").toURI();
    String devMultiplePipelines = new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);

    List<String> multiplePipelines = new ArrayList<>();
    for (int i = 1; i <= 4; i++) {
      multiplePipelines.add(
        devMultiplePipelines.replaceAll("\"name\" : \"dev_multiple_pipelines\"",
          "\"name\" : \"dev_multiple_pipelines" + i + "\""));
    }
    return multiplePipelines;
  }

  protected Map<String, String> getPipelineNameAndRev() {
    Map<String, String> map = new HashMap<>();
    for (int i = 1; i <= 4; i++) {
      map.put("dev_multiple_pipelines" + i, "0");
    }
    return map;
  }

  /**
   * The extending test must call this method in the method scheduled to run before class
   * @throws Exception
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    TestMultiplePipelinesBase.beforeClass(getPipelineJson());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TestMultiplePipelinesBase.afterClass();
  }

}
