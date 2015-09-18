/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.creation;

import java.util.List;

public class PipelineBean {
  private final PipelineConfigBean config;
  private final List<StageBean> stages;
  private final StageBean errorStage;

  public PipelineBean(PipelineConfigBean config, List<StageBean> stages, StageBean errorStage) {
    this.config = config;
    this.stages = stages;
    this.errorStage = errorStage;
  }

  public PipelineConfigBean getConfig() {
    return config;
  }

  public List<StageBean> getStages() {
    return stages;
  }

  public StageBean getErrorStage() {
    return errorStage;
  }

}
