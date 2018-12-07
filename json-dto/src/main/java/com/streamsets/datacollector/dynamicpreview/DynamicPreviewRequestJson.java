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

package com.streamsets.datacollector.dynamicpreview;

import java.util.HashMap;
import java.util.Map;

public class DynamicPreviewRequestJson {
  private DynamicPreviewType type;
  private Map<String, Object> parameters = new HashMap<>();
  private int batchSize;
  private int batches;
  private boolean skipTargets;
  private boolean skipLifecycleEvents;
  private String endStageInstanceName;
  private long timeout;
  private boolean testOrigin;

  /**
   * This holds the List&lt;StageOutput&gtl; which are the stage output overrides (i.e. user editable records in a
   * preview that can be re-run). It is stored as a String representation of a JSON structure. This is because the
   * various classes involved (i.e. StageOutputJson, RecordJson, etc.) all live in the container module and can't
   * easily be refactored into a module that is shared between the SDC and SCH code base (ex: this one, json-dto)
   */
  private String stageOutputsToOverrideJsonText;

  public DynamicPreviewType getType() {
    return type;
  }

  public void setType(DynamicPreviewType type) {
    this.type = type;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }
  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getBatches() {
    return batches;
  }

  public void setBatches(int batches) {
    this.batches = batches;
  }

  public boolean isSkipTargets() {
    return skipTargets;
  }

  public void setSkipTargets(boolean skipTargets) {
    this.skipTargets = skipTargets;
  }

  public boolean isSkipLifecycleEvents() {
    return skipLifecycleEvents;
  }

  public void setSkipLifecycleEvents(boolean skipLifecycleEvents) {
    this.skipLifecycleEvents = skipLifecycleEvents;
  }

  public String getEndStageInstanceName() {
    return endStageInstanceName;
  }

  public void setEndStageInstanceName(String endStageInstanceName) {
    this.endStageInstanceName = endStageInstanceName;
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public boolean isTestOrigin() {
    return testOrigin;
  }

  public void setTestOrigin(boolean testOrigin) {
    this.testOrigin = testOrigin;
  }

  public String getStageOutputsToOverrideJsonText() {
    return stageOutputsToOverrideJsonText;
  }

  public void setStageOutputsToOverrideJsonText(String stageOutputsToOverrideJsonText) {
    this.stageOutputsToOverrideJsonText = stageOutputsToOverrideJsonText;
  }
}
