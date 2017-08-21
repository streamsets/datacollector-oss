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
