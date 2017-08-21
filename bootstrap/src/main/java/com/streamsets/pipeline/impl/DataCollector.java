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
package com.streamsets.pipeline.impl;

import com.streamsets.pipeline.validation.ValidationIssue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public interface DataCollector {

  void init() throws Exception;

  void destroy();

  URI getServerURI();

  void startPipeline(String pipelineJson) throws Exception;

  void createPipeline(String pipelineJson) throws Exception;

  List<? extends ValidationIssue> validatePipeline(String name, String pipelineJson) throws IOException;

  void startPipeline() throws Exception;

  void stopPipeline() throws Exception;

  List<URI> getWorkerList() throws URISyntaxException;

  public String storeRules(String name, String tag, String ruleDefinition) throws Exception;
}
