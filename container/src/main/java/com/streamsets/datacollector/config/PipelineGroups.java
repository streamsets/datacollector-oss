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

import com.streamsets.pipeline.api.Label;

public enum PipelineGroups implements Label {
  // All Transformer configs are on this tab/group - some of them can be basic, some of them can be advanced
  CLUSTER("Cluster"),
  // All SDC Configs are on this group/tab - all configs here should be advanced since they depend on executionMode
  // that is itself advanced.
  CLUSTER_SDC("Cluster"),
  PARAMETERS("Parameters"),
  NOTIFICATIONS("Notifications"),
  BAD_RECORDS("Error Records"),
  STATS("Statistics"),
  // EMR Tab is only used by SDC, Tranformer shows EMR configs on "CLUSTER" tab like all other cluster types
  EMR("EMR"),
  ADVANCED("Advanced"),
  ;

  private final String label;

  PipelineGroups(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
