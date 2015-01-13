/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.configuration;

import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
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
        bindFactory(RuntimeInfoInjector.class).to(RuntimeInfo.class);
        bindFactory(BuildInfoInjector.class).to(BuildInfo.class);
        bindFactory(ProductionPipelineManagerInjector.class).to(ProductionPipelineManagerTask.class);
      }
    });
  }

}
