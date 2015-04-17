/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.domainServer;

import com.streamsets.pipeline.main.RuntimeModule;
import com.streamsets.pipeline.task.Task;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(library = true, includes = {RuntimeModule.class})
public class DomainServerCallbackModule {

  @Provides
  @Singleton
  public Task provideDomainServerCallback(DomainServerCallbackTask domainServerCallbackTask) {
    return domainServerCallbackTask;
  }

}
