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
package com.streamsets.pipeline.restapi.configuration;

import com.streamsets.pipeline.prodmanager.PipelineProductionManagerTask;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.util.Configuration;
import com.streamsets.pipeline.store.PipelineStoreTask;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.URI;
import java.security.Principal;

public class RestAPIResourceConfig extends ResourceConfig {

  public RestAPIResourceConfig() {
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bindFactory(PipelineStoreInjector.class).to(PipelineStoreTask.class);
        bindFactory(StageLibraryInjector.class).to(StageLibraryTask.class);
        bindFactory(PrincipalInjector.class).to(Principal.class);
        bindFactory(URIInjector.class).to(URI.class);
        bindFactory(ConfigurationInjector.class).to(Configuration.class);
        bindFactory(PipelineStateMgrInjector.class).to(PipelineProductionManagerTask.class);
      }
    });
  }

}
