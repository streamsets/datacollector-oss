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
package com.streamsets.datacollector.multiple;

import com.google.common.io.Resources;
import com.streamsets.datacollector.base.MultiplePipelinesBaseIT;

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

public class MultiplePipelinesDevIT extends MultiplePipelinesBaseIT {

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
    MultiplePipelinesBaseIT.beforeClass(getPipelineJson());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    MultiplePipelinesBaseIT.afterClass();
  }

}
