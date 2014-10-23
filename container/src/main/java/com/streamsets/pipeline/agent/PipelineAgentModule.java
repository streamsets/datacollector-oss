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
package com.streamsets.pipeline.agent;

import com.streamsets.pipeline.http.WebServerModule;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module(injects = {MainAgent.class, LogConfigurator.class, BuildInfo.class, RuntimeInfo.class},
        includes = {RuntimeModule.class, WebServerModule.class})
public class PipelineAgentModule {

  @Provides @Singleton
  public Agent provideAgent(PipelineAgent agent) {
    return agent;
  }

}
