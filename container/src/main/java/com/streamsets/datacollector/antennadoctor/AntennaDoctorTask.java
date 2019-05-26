/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.antennadoctor;

import com.streamsets.datacollector.antennadoctor.engine.context.AntennaDoctorContext;
import com.streamsets.datacollector.antennadoctor.engine.context.AntennaDoctorStageContext;
import com.streamsets.datacollector.task.Task;
import com.streamsets.pipeline.api.AntennaDoctorMessage;
import com.streamsets.pipeline.api.ErrorCode;

import java.util.List;

/**
 * Antenna Doctor is a rule-based engine for self-served help when something goes unexpectedly
 * wrong in the product or pipeline execution.
 */
public interface AntennaDoctorTask extends Task {
  /**
   * Return context for the Antenna Doctor execution.
   */
  public AntennaDoctorContext getContext();

  // Stage related methods
  public List<AntennaDoctorMessage> onStage(AntennaDoctorStageContext context, Exception exception);
  public List<AntennaDoctorMessage> onStage(AntennaDoctorStageContext context, ErrorCode errorCode, Object... args);
  public List<AntennaDoctorMessage> onStage(AntennaDoctorStageContext context, String errorMessage);

  // Rest related methods - no explicit context as the SDC is used
  public List<AntennaDoctorMessage> onRest(ErrorCode errorCode, Object... args);
  public List<AntennaDoctorMessage> onRest(Exception exception);

  // Validation
  public List<AntennaDoctorMessage> onValidation(AntennaDoctorStageContext context, String groupName, String configName, ErrorCode errorCode, Object... args);
}
