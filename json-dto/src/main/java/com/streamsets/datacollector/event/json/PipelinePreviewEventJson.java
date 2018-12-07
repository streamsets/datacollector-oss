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
package com.streamsets.datacollector.event.json;

public class PipelinePreviewEventJson extends PipelineStartEventJson {
  private int batches;
  private int batchSize;
  private boolean skipTargets;
  private boolean skipLifecycleEvents;
  private String stopStage;
  private String stageOutputsToOverrideJsonText;
  private long timeoutMillis;
  private boolean testOrigin;

  public PipelinePreviewEventJson() {

  }

  public PipelinePreviewEventJson(
      int batches,
      int batchSize,
      boolean skipTargets,
      boolean skipLifecycleEvents,
      String stopStage,
      long timeoutMillis,
      boolean testOrigin,
      String stageOutputsToOverrideJsonText
  ) {
    this.batches = batches;
    this.batchSize = batchSize;
    this.skipTargets = skipTargets;
    this.skipLifecycleEvents = skipLifecycleEvents;
    this.stopStage = stopStage;
    this.timeoutMillis = timeoutMillis;
    this.testOrigin = testOrigin;
    this.stageOutputsToOverrideJsonText = stageOutputsToOverrideJsonText;
  }

  public int getBatches() {
    return batches;
  }

  public void setBatches(int batches) {
    this.batches = batches;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
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

  public String getStopStage() {
    return stopStage;
  }

  public void setStopStage(String stopStage) {
    this.stopStage = stopStage;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public void setTimeoutMillis(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
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
