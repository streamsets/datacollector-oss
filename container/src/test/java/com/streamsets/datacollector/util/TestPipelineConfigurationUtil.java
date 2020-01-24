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
package com.streamsets.datacollector.util;

import com.google.common.io.Resources;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPipelineConfigurationUtil {

  @Test
  public void testRetrieveSourceLibrary() throws IOException, URISyntaxException {
    String pipelineJson =
      new String(Files.readAllBytes(Paths.get(Resources.getResource("sample_pipeline.json").toURI())),
        StandardCharsets.UTF_8);
    assertEquals("streamsets-datacollector-cdh5_4_0-lib", PipelineConfigurationUtil.getSourceLibName(pipelineJson));
  }

  @Test
  public void testGeneratePipelineId() {
    String pipelineId = PipelineConfigurationUtil.generatePipelineId("samplePipeline12345");
    assertEquals(46, pipelineId.length());
    assertTrue(pipelineId.startsWith("samplePipe"));
    assertFalse(pipelineId.startsWith("samplePipeline12345"));
  }
}
