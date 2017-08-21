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
package com.streamsets.datacollector.execution.runner.provider.dagger;

import com.streamsets.datacollector.execution.manager.RunnerProvider;
import com.streamsets.datacollector.execution.runner.provider.StandaloneAndClusterRunnerProviderImpl;

import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of RunnerProvider.
 */
@Module(injects = RunnerProvider.class, library = true)
public class StandaloneAndClusterRunnerProviderModule {

  @Provides
  @Singleton
  public RunnerProvider provideRunnerProvider(StandaloneAndClusterRunnerProviderImpl runnerProvider) {
    return runnerProvider;
  }
}
