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
package com.streamsets.datacollector.creation;

public class PipelineBean {
  private final PipelineConfigBean config;
  private final StageBean origin;
  private final PipelineStageBeans stages;
  private final StageBean errorStage;
  private final StageBean statsAggregatorStage;
  private final PipelineStageBeans startEventStages;
  private final PipelineStageBeans stopEventStages;

  PipelineBean(
      PipelineConfigBean config,
      StageBean origin,
      PipelineStageBeans stages,
      StageBean errorStage,
      StageBean statsAggregatorStage,
      PipelineStageBeans startEventStages,
      PipelineStageBeans stopEventStages
  ) {
    this.config = config;
    this.origin = origin;
    this.stages = stages;
    this.errorStage = errorStage;
    this.statsAggregatorStage = statsAggregatorStage;
    this.startEventStages = startEventStages;
    this.stopEventStages = stopEventStages;
  }

  public PipelineConfigBean getConfig() {
    return config;
  }

  public StageBean getOrigin() {
    return origin;
  }

  public PipelineStageBeans getPipelineStageBeans() {
    return stages;
  }

  public StageBean getErrorStage() {
    return errorStage;
  }

  public StageBean getStatsAggregatorStage() {
    return statsAggregatorStage;
  }

  public PipelineStageBeans getStartEventStages() {
    return startEventStages;
  }

  public PipelineStageBeans getStopEventStages() {
    return stopEventStages;
  }
}
