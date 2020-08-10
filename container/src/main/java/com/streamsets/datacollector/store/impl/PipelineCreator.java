/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.store.impl;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.PipelineDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreException;

import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.function.Supplier;

public class PipelineCreator {
  static final String REV = "0";

  private final PipelineDefinition pipelineDefinition;
  private final int schemaVersion;
  private final String sdcVersion;
  private final String sdcId;
  private final Supplier<StageConfiguration> defaultStatsAggrInstanceSupplier;
  private final Supplier<StageConfiguration> defaultTestOriginStageInstanceSupplier;
  private final Supplier<StageConfiguration> defaultErrorStageInstanceSupplier;


  public PipelineCreator(
      PipelineDefinition pipelineDefinition,
      int schemaVersion,
      String sdcVersion,
      String sdcId,
      Supplier<StageConfiguration> defaultStatsAggrInstanceSupplier,
      Supplier<StageConfiguration> defaultTestOriginStageInstanceSupplier,
      Supplier<StageConfiguration> defaultErrorStageInstanceSupplier
  ) {
    this.pipelineDefinition = pipelineDefinition;
    this.schemaVersion = schemaVersion;
    this.sdcVersion = sdcVersion;
    this.sdcId = sdcId;
    this.defaultStatsAggrInstanceSupplier = defaultStatsAggrInstanceSupplier;
    this.defaultTestOriginStageInstanceSupplier = defaultTestOriginStageInstanceSupplier;
    this.defaultErrorStageInstanceSupplier = defaultErrorStageInstanceSupplier;
  }

  public PipelineConfiguration create(String user, String pipelineId, String pipelineTitle, String description, Date date) {
      UUID uuid = UUID.randomUUID();
      PipelineInfo info = new PipelineInfo(
          pipelineId,
          pipelineTitle,
          description,
          date,
          date,
          user,
          user,
          REV,
          uuid,
          false,
          null,
          sdcVersion,
          sdcId
      );

      PipelineConfiguration pipeline = new PipelineConfiguration(
          schemaVersion,
          PipelineConfigBean.VERSION,
          pipelineId,
          uuid,
          pipelineTitle,
          description,
          pipelineDefinition.getPipelineDefaultConfigs(),
          Collections.emptyMap(),
          null,
          Collections.emptyList(),
          defaultErrorStageInstanceSupplier.get(),
          defaultStatsAggrInstanceSupplier.get(),
          Collections.emptyList(),
          Collections.emptyList(),
          defaultTestOriginStageInstanceSupplier.get()
      );

      pipeline.setPipelineInfo(info);
      return pipeline;
  }

}
