/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

import com.google.common.io.Resources;

public class TestPipelineConfigurationUtil {

  @Test
  public void testRetrieveSourceLibrary() throws IOException, URISyntaxException {
    String pipelineJson =
      new String(Files.readAllBytes(Paths.get(Resources.getResource("sample_pipeline.json").toURI())),
        StandardCharsets.UTF_8);
    assertEquals("streamsets-datacollector-cdh5_4_0-lib", PipelineConfigurationUtil.getSourceLibName(pipelineJson));
  }
}
