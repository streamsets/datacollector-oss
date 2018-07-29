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
package com.streamsets.datacollector.cluster;

import com.google.common.collect.ImmutableMap;

import java.util.Properties;

public class EmrStatusParser {

  private static final String WAITING = "WAITING";
  private static final String STARTING = "STARTING";
  private static final String RUNNING = "RUNNING";

  private static final String BOOTSTRAPPING = "BOOTSTRAPPING";
  private static final String TERMINATING = "TERMINATING";
  private static final String TERMINATED = "TERMINATED";
  private static final String PENDING = "PENDING";
  private static final String TERMINATED_WITH_ERRORS = "TERMINATED_WITH_ERRORS";
  private static final String SUCCEEDED = "SUCCEEDED";
  private static final String FAILED = "FAILED";
  private static final String KILLED = "KILLED";
  private static final String CANCEL_PENDING = "CANCEL_PENDING";
  private static final String COMPLETED = "COMPLETED";
  private static final String CANCELLED = "CANCELLED";
  private static final String INTERRUPTED = "INTERRUPTED";
  private static final String NEW = "NEW";
  private static final String ACCEPTED = "ACCEPTED";


  private static final ImmutableMap<String, ClusterPipelineStatus> CLUSTER_STATE_MAP = ImmutableMap.<String, ClusterPipelineStatus>builder()
      .put(WAITING, ClusterPipelineStatus.RUNNING)
      .put(RUNNING, ClusterPipelineStatus.RUNNING)
      .put(STARTING, ClusterPipelineStatus.STARTING)
      .put(BOOTSTRAPPING, ClusterPipelineStatus.STARTING)
      .put(TERMINATING, ClusterPipelineStatus.KILLED)
      .put(TERMINATED, ClusterPipelineStatus.KILLED)
      .put(TERMINATED_WITH_ERRORS, ClusterPipelineStatus.KILLED).build();

  private static final ImmutableMap<String, ClusterPipelineStatus> EMR_STEP_STATE_MAP = ImmutableMap.<String, ClusterPipelineStatus>builder()
      .put(PENDING, ClusterPipelineStatus.STARTING)
      .put(CANCEL_PENDING , ClusterPipelineStatus.STARTING) // emr step can transition from cancel_pending to running
      .put(RUNNING, ClusterPipelineStatus.RUNNING)
      .put(COMPLETED, ClusterPipelineStatus.RUNNING)
      .put(CANCELLED, ClusterPipelineStatus.KILLED)
      .put(INTERRUPTED, ClusterPipelineStatus.FAILED)
      .put(FAILED, ClusterPipelineStatus.FAILED)
      .build();

  private static final ImmutableMap<String, ClusterPipelineStatus> YARN_STATUS_STATE_MAP = ImmutableMap.<String, ClusterPipelineStatus>builder()
      .put(NEW, ClusterPipelineStatus.RUNNING)
      .put(ACCEPTED, ClusterPipelineStatus.RUNNING)
      .put(SUCCEEDED, ClusterPipelineStatus.SUCCEEDED)
      .put(KILLED, ClusterPipelineStatus.KILLED).build();

  private static final  ImmutableMap<String, ClusterPipelineStatus> JOB_STATUS_STATE_MAP = ImmutableMap.<String, ClusterPipelineStatus>builder()
      .putAll(EMR_STEP_STATE_MAP).putAll(YARN_STATUS_STATE_MAP).build();

  public static ClusterPipelineStatus parseClusterStatus(Properties emrClusterStateProps) {
    return CLUSTER_STATE_MAP.get(emrClusterStateProps.get("state"));
  }

  public static ClusterPipelineStatus parseJobStatus(Properties emrJobStateProps) {
    return JOB_STATUS_STATE_MAP.get(emrJobStateProps.get("state"));
  }
}
