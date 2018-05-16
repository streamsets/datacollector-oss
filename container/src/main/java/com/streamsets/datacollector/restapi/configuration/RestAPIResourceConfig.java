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
package com.streamsets.datacollector.restapi.configuration;

import com.streamsets.datacollector.activation.Activation;
import com.streamsets.datacollector.bundles.SupportBundleManager;
import com.streamsets.datacollector.execution.Manager;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.restapi.RestAPI;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.AclStoreTask;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import io.swagger.models.Info;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.CsrfProtectionFilter;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;

import java.net.URI;
import java.security.Principal;

public class RestAPIResourceConfig extends ResourceConfig {

  public RestAPIResourceConfig() {
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bindFactory(PipelineStoreInjector.class).to(PipelineStoreTask.class);
        bindFactory(AclStoreInjector.class).to(AclStoreTask.class);
        bindFactory(StageLibraryInjector.class).to(StageLibraryTask.class);
        bindFactory(PrincipalInjector.class).to(Principal.class);
        bindFactory(URIInjector.class).to(URI.class);
        bindFactory(ConfigurationInjector.class).to(Configuration.class);
        bindFactory(RuntimeInfoInjector.class).to(RuntimeInfo.class);
        bindFactory(BuildInfoInjector.class).to(BuildInfo.class);
        bindFactory(StatsCollectorInjector.class).to(StatsCollector.class);
        bindFactory(StandAndClusterManagerInjector.class).to(Manager.class);
        bindFactory(SupportBundleInjector.class).to(SupportBundleManager.class);
        bindFactory(UserGroupManagerInjector.class).to(UserGroupManager.class);
        bindFactory(ActivationInjector.class).to(Activation.class);
      }
    });

    register(RolesAllowedDynamicFeature.class);
    register(CsrfProtectionFilter.class);
    register(MultiPartFeature.class);

    //Hooking up Swagger-Core
    register(ApiListingResource.class);
    register(SwaggerSerializers.class);

    //Configure and Initialize Swagger
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setVersion("1.0.0");
    beanConfig.setBasePath("/rest");
    beanConfig.setResourcePackage(RestAPI.class.getPackage().getName());
    beanConfig.setScan(true);
    beanConfig.setTitle("Data Collector RESTful API");

    Info info = new Info();
    info.setTitle("Data Collector RESTful API");
    info.setDescription("An Enterprise-Grade Approach to Managing Big Data in Motion");
    info.setVersion("1.0.0");
    beanConfig.setInfo(info);
  }

}
