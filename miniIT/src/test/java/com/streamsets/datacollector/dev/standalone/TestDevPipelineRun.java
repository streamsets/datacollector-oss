/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.streamsets.datacollector.base.TestPipelineRunStandalone;

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

  @Override
  protected String getPipelineName() {
    return "admin";
  }

  @Override
  protected String getPipelineRev() {
    return "0";
  }

}
