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
package com.streamsets.datacollector.cluster;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestMesosStatusParser {

  @Test
  public void testValidOutput() throws Exception {
    assertValidOutput("/mesos-status-failed.txt", "FAILED");
    assertValidOutput("/mesos-status-queued.txt", "RUNNING");
    assertValidOutput("/mesos-status-finished.txt", "SUCCEEDED");
    assertValidOutput("/mesos-status-killed.txt", "KILLED");
    assertValidOutput("/mesos-status-lost.txt", "FAILED");
  }

  private static void assertValidOutput(String name, String output) throws Exception {
    MesosStatusParser parser = new MesosStatusParser();
    Assert.assertEquals(output, parser.parseStatus(readFile(name)));
    ClusterPipelineStatus.valueOf(output);
  }

  private static List<String> readFile(String name) throws Exception {
    return Resources.readLines(TestMesosStatusParser.class.getResource(name),
      StandardCharsets.UTF_8);
  }

}
