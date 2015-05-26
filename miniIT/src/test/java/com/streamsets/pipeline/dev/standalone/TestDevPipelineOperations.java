/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.dev.standalone;

import com.google.common.io.Resources;
import com.streamsets.pipeline.base.TestPipelineOperationsStandalone;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestDevPipelineOperations extends TestPipelineOperationsStandalone {

  @BeforeClass
  public static void beforeClass() throws Exception {
    TestPipelineOperationsStandalone.beforeClass(getPipelineJson());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TestPipelineOperationsStandalone.afterClass();
  }

  private static String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("dev_pipeline_operations.json").toURI();
    return new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
  }

  @Override
  protected String getPipelineName() {
    return "admin";
  }

  @Override
  protected String getPipelineRev() {
    return "0";
  }

  @Override
  protected void postPipelineStart() {

  }

}
