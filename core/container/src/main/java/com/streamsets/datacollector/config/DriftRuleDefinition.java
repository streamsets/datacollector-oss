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
package com.streamsets.datacollector.config;

import com.streamsets.datacollector.el.RuleELRegistry;

public class DriftRuleDefinition extends DataRuleDefinition {

  public DriftRuleDefinition(
      String id,
      String label,
      String lane,
      double samplingPercentage,
      int samplingRecordsToRetain,
      String condition,
      boolean alertEnabled,
      String alertText,
      boolean meterEnabled,
      boolean sendEmail,
      boolean enabled,
      long timestamp
  ) {
    super(
        RuleELRegistry.DRIFT,
        id,
        label,
        lane,
        samplingPercentage,
        samplingRecordsToRetain,
        condition,
        alertEnabled,
        alertText,
        ThresholdType.COUNT,
        "0",
        1,
        meterEnabled,
        sendEmail,
        enabled,
        timestamp
    );
  }
}
