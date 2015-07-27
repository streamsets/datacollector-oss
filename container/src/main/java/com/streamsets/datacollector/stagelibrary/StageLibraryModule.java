/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.stagelibrary;

import com.streamsets.datacollector.main.RuntimeModule;

import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = StageLibraryTask.class, library = true, includes = {RuntimeModule.class})
public class StageLibraryModule {

  @Provides
  @Singleton
  public StageLibraryTask provideStageLibrary(ClassLoaderStageLibraryTask stageLibrary) {
    return stageLibrary;
  }

}
