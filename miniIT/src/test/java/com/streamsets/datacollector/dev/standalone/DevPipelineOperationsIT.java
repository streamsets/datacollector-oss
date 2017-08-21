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
package com.streamsets.datacollector.dev.standalone;

import com.google.common.io.Resources;
import com.streamsets.datacollector.base.PipelineOperationsStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DevPipelineOperationsIT extends PipelineOperationsStandaloneIT {

  @BeforeClass
  public static void beforeClass() throws Exception {
    PipelineOperationsStandaloneIT.beforeClass(getPipelineJson());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    PipelineOperationsStandaloneIT.afterClass();
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
