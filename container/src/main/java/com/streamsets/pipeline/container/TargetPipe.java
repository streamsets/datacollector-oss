/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.container;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Module.Info;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.Configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class TargetPipe extends Pipe implements Target.Context {
  private static final Set<String> EMPTY_OUTPUT = new HashSet<String>();

  private Target target;

  public TargetPipe(List<Info> pipelineInfo, MetricRegistry metrics, Module.Info moduleInfo, Target target,
      Set<String> inputLanes) {
    super(pipelineInfo, metrics, moduleInfo, inputLanes, EMPTY_OUTPUT);
    Preconditions.checkNotNull(target, "target cannot be null");
    this.target = target;
  }

  @Override
  public void init() {
    target.init(getModuleInfo(), this);
  }

  @Override
  public void destroy() {
    target.destroy();
  }

  @Override
  public void configure(Configuration conf) {
    super.configure(conf);
    //TODO
  }

  @Override
  protected void processBatch(PipeBatch batch) {
    Preconditions.checkNotNull(batch, "batch cannot be null");
    target.write(batch);
    //LOG warning if !batch.isInputFullyConsumed()
  }

}
