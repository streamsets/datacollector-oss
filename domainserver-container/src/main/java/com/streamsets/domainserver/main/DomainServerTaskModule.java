/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.domainserver.main;

import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.LogConfigurator;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.task.Task;
import com.streamsets.pipeline.task.TaskWrapper;
import com.streamsets.domainserver.http.WebServerModule;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;


@Module(injects = {TaskWrapper.class, LogConfigurator.class, BuildInfo.class, RuntimeInfo.class},
  includes = {RuntimeModule.class, WebServerModule.class})
public class DomainServerTaskModule {

  @Provides
  @Singleton
  public Task provideDomainServerTask(DomainServerTask domainServerTask) {
    return domainServerTask;
  }

}
