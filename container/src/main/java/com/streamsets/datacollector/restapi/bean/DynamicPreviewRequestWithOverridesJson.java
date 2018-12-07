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

package com.streamsets.datacollector.restapi.bean;

import com.streamsets.datacollector.dynamicpreview.DynamicPreviewRequestJson;

import java.util.List;

/**
 * A wrapper that combines the {@link DynamicPreviewRequestJson} and list of stage overrides (via
 * {@link StageOutputJson}). This class lives in container since that is the only module in which
 * all of these are available at the same time.
 */
public class DynamicPreviewRequestWithOverridesJson {
  private List<StageOutputJson> stageOutputsToOverrideJson;
  private DynamicPreviewRequestJson dynamicPreviewRequestJson;

  public List<StageOutputJson> getStageOutputsToOverrideJson() {
    return stageOutputsToOverrideJson;
  }

  public void setStageOutputsToOverrideJson(List<StageOutputJson> stageOutputsToOverrideJson) {
    this.stageOutputsToOverrideJson = stageOutputsToOverrideJson;
  }

  public DynamicPreviewRequestJson getDynamicPreviewRequestJson() {
    return dynamicPreviewRequestJson;
  }

  public void setDynamicPreviewRequestJson(DynamicPreviewRequestJson dynamicPreviewRequestJson) {
    this.dynamicPreviewRequestJson = dynamicPreviewRequestJson;
  }
}
