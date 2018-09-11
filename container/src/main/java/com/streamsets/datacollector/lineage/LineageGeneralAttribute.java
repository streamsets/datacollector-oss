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
package com.streamsets.datacollector.lineage;

import com.streamsets.pipeline.api.Label;

public enum LineageGeneralAttribute implements Label {

  EVENT_TYPE("eventType"),

  // name in the UI, changed when pipeline is renamed,
  PIPELINE_TITLE("title"),

  //pipeline unique ID: devlocal99cf1aef-b609-4e2a-88ee-4ea436aafb93
  PIPELINE_ID("Id"),
  PIPELINE_USER("user"),
  PIPELINE_VERSION("pipeline_version"),
  PIPELINE_LABELS("labels"),
  PIPELINE_PARAMETERS("pipeline_parameters"),

  // This description is used in the Description space hence it won't go in to custom metadata
  PIPELINE_DESCRIPTION("description"),

  // this timestamp is pulled from system time,
  // and it may be posted to the UI.
  // when we receive file rollover events, this
  // my be interesting information.
  TIME_STAMP("timeStamp"),

  // used to make a unique id which represents
  // this specific run of this pipeline.
  PIPELINE_START_TIME("pipelineStartTime"),

  // id of this Data Collector instance.
  SDC_ID("Sdc_Id"),

  PERMALINK("permaLink"),

  // probably will not be displayed, but we need this to
  // build a unique identity hash for each stage.
  // We MD5 hash the pipelineID, stage name, and pipelineStartTime to
  // make the unique identity value for this specific stage
  // during this specific pipeline run.
  STAGE_NAME("stageName"),
  ;

  private String label;

  LineageGeneralAttribute(String label){
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }

}
