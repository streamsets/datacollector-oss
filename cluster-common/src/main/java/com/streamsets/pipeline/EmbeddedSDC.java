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
package com.streamsets.pipeline;

import com.streamsets.pipeline.api.impl.ClusterSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Embedded SDC providing access to the source
 */
public class EmbeddedSDC {
  private static final AtomicInteger instanceIdCounter = new AtomicInteger(0);
  private final int instanceId;
  private ClusterSource source;

  // To avoid having SparkProcessors in classpath for Cluster batch mode, we use objects here.
  private List<Object> sparkProcessors;

  public EmbeddedSDC() {
    instanceId = instanceIdCounter.getAndIncrement();
  }

  public int getInstanceId() {
    return instanceId;
  }

  public ClusterSource getSource() {
    return source;
  }

  public void setSource(ClusterSource source) {
    this.source = source;
  }

  public void setSparkProcessors(List<Object> processors) {
    this.sparkProcessors = processors;
  }

  public Object getSparkProcessorAt(int id) {
    return id < sparkProcessors.size() ? sparkProcessors.get(id) : null;
  }

  public boolean inErrorState() {
    return source != null && source.inErrorState();
  }
}
