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

import com.google.common.io.Resources;
import com.streamsets.datacollector.cluster.ClusterPipelineStatus;
import com.streamsets.datacollector.cluster.YARNStatusParser;
import com.streamsets.datacollector.util.MiniSDCSystemProcessImpl;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class TestYARNStatusParser {

  @Test
  public void testValidOutput() throws Exception {
    assertValidOutput("/yarn-status-success.txt", "SUCCEEDED");
    assertValidOutput("/yarn-status-running.txt", "RUNNING");
    assertValidOutput("/yarn-status-failed.txt", "FAILED");
    YARNStatusParser parser = new YARNStatusParser();
    Assert.assertEquals("RUNNING", parser.parseStatus(Arrays.asList(MiniSDCSystemProcessImpl.YARN_STATUS_SUCCESS)));
  }

  @Test
  public void testKilledOutput() throws Exception {
    assertValidOutput("/yarn-status-killed.txt", "KILLED");
  }

  @Test
  public void testNewOutput() throws Exception {
    assertValidOutput("/yarn-status-new.txt", "RUNNING");
  }

  private static void assertValidOutput(String name, String output) throws Exception {
    YARNStatusParser parser = new YARNStatusParser();
    Assert.assertEquals(output, parser.parseStatus(readFile(name)));
    ClusterPipelineStatus.valueOf(output);
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidOutput() throws Exception {
    YARNStatusParser parser = new YARNStatusParser();
    parser.parseStatus(Arrays.<String>asList());
  }

  private static List<String> readFile(String name) throws Exception {
    return Resources.readLines(TestYARNStatusParser.class.getResource(name),
      StandardCharsets.UTF_8);
  }
}
