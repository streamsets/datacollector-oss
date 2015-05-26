/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.dev.standalone;

import com.google.common.io.Resources;
import com.streamsets.pipeline.base.TestPipelineRunStandalone;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestDevPipelineRun extends TestPipelineRunStandalone {

  @Override
  protected String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("dev_pipeline_run.json").toURI();
    return new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
  }

  @Override
  protected int getRecordsInOrigin() {
    return 500;
  }

  @Override
  protected int getRecordsInTarget() {
    return 500;
  }

}
