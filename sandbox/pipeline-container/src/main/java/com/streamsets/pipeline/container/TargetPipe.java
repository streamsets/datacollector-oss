/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
