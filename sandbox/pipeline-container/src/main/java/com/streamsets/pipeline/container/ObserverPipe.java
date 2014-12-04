/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.container;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.config.Configuration;

class ObserverPipe extends Pipe {

  private Observer observer;

  private static ModuleInfo createObserverInfo(Module.Info info) {
    return new ModuleInfo("Observer", "-", "Pipeline built-in observer", info.getInstanceName() + ":observer", false);
  }

  private ObserverPipe(Pipe pipe, Observer observer) {
    super(pipe.getPipelineInfo(), pipe.getMetrics(), createObserverInfo(pipe.getModuleInfo()),
          pipe.getOutputLanes(), pipe.getOutputLanes());
    Preconditions.checkNotNull(observer, "observer cannot be null");
    this.observer = observer;
  }

  public ObserverPipe(SourcePipe pipe, Observer observer) {
    this((Pipe)pipe, observer);
  }

  public ObserverPipe(ProcessorPipe pipe, Observer observer) {
    this((Pipe)pipe, observer);
  }

  @Override
  public void init() {
    observer.init();
  }

  @Override
  public void destroy() {
    observer.destroy();
  }

  @Override
  public void configure(Configuration conf) {
    Preconditions.checkNotNull(conf, "conf cannot be null");
    observer.configure(conf);
  }

  @Override
  protected void processBatch(PipeBatch batch) {
    Preconditions.checkNotNull(batch, "batch cannot be null");
    if (observer.isActive()) {
      observer.observe(batch);
    }
  }

}
