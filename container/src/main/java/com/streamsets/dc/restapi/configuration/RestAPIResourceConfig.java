/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.restapi.configuration;

import com.streamsets.dc.execution.Manager;
import com.streamsets.pipeline.main.BuildInfo;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.dc.restapi.RestAPI;
import com.streamsets.pipeline.restapi.configuration.BuildInfoInjector;
import com.streamsets.pipeline.restapi.configuration.ConfigurationInjector;
import com.streamsets.pipeline.restapi.configuration.PipelineStoreInjector;
import com.streamsets.pipeline.restapi.configuration.PrincipalInjector;
import com.streamsets.pipeline.restapi.configuration.RuntimeInfoInjector;
import com.streamsets.pipeline.restapi.configuration.StageLibraryInjector;
import com.streamsets.pipeline.restapi.configuration.URIInjector;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;

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
        bindFactory(StandAndClusterManagerInjector.class).to(Manager.class);
      }
    });

    register(RolesAllowedDynamicFeature.class);

    //Hooking up Swagger-Core
    register(ApiListingResource.class);
    register(SwaggerSerializers.class);

    //Configure and Initialize Swagger
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setVersion("1.0.0");
    beanConfig.setBasePath("/rest");
    beanConfig.setResourcePackage(RestAPI.class.getPackage().getName());
    beanConfig.setScan(true);
  }

}
