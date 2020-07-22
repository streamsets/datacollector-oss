/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.datacollector.restapi.configuration.JsonConfigurator;
import com.streamsets.datacollector.runner.MockStages;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.security.Principal;

public class TestPipelineUpgraderResource extends JerseyTest {

  static class TestResourceConfig extends AbstractBinder {

    @Override
    protected void configure() {
      bindFactory(TestUtil.StageLibraryTestInjector.class).to(StageLibraryTask.class);
      bindFactory(TestUtil.BuildInfoTestInjector.class).to(BuildInfo.class);
      bindFactory(ConfigurationTestInjector.class).to(Configuration.class);
      bindFactory(TestPipelineStoreResource.RuntimeInfoTestInjector.class).to(RuntimeInfo.class);
      bindFactory(TestUtil.PrincipalTestInjector.class).to(Principal.class);
    }

  }

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(JsonConfigurator.class);
        register(PipelineUpgraderResource.class);
        register(new TestResourceConfig());
      }
    };
  }


  @Test
  public void testUpgrade() throws Exception {
    Response response = null;
    try {
      PipelineConfiguration conf = MockStages.createPipelineConfigurationSourceTarget();
      int version = conf.getVersion();
      PipelineConfigurationJson json = BeanHelper.wrapPipelineConfiguration(conf);
      response = target("/v1/pipeline-upgrader").request().post(Entity.json(json));
      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      json = ObjectMapperFactory.get().readValue((InputStream)response.getEntity(), PipelineConfigurationJson.class);
      Assert.assertNotNull(json);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }


}
