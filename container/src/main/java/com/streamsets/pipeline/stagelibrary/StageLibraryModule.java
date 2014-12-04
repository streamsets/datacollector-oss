/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stagelibrary;

import com.streamsets.pipeline.main.RuntimeModule;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(library = true, includes = {RuntimeModule.class})
public class StageLibraryModule {

  @Provides
  @Singleton
  public StageLibraryTask provideStageLibrary(ClassLoaderStageLibraryTask stageLibrary) {
    return stageLibrary;
  }

}
