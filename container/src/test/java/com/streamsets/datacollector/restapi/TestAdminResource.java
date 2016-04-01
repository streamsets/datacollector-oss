/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.main.RuntimeInfo;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class TestAdminResource extends JerseyTest {

  private static File confDir;

  @Test
  public void testUpdateAppToken() throws IOException {
    Response response = target("/v1/system/appToken").request().header("X-Requested-By", "SDC").post(Entity.text("ola"));
    Assert.assertEquals(200, response.getStatus());

    // read contents of application-token.txt
    File appTokenFIle = new File(confDir, "application-token.txt");
    Assert.assertTrue(appTokenFIle.exists());
    List<String> strings = IOUtils.readLines(new FileInputStream(appTokenFIle));
    Assert.assertEquals(1, strings.size());
    Assert.assertEquals("ola", strings.get(0));

    response = target("/v1/system/appToken").request().header("X-Requested-By", "SDC").post(Entity.text("como esta"));
    Assert.assertEquals(200, response.getStatus());

    // read contents of application-token.txt
    appTokenFIle = new File(confDir, "application-token.txt");
    strings = IOUtils.readLines(new FileInputStream(appTokenFIle));
    Assert.assertEquals(1, strings.size());
    Assert.assertEquals("como esta", strings.get(0));
  }

  @Override
  protected Application configure() {
    return new ResourceConfig() {
      {
        register(new AdminResourceConfig());
        register(AdminResource.class);
      }
    };
  }

  static class AdminResourceConfig extends AbstractBinder {
    @Override
    protected void configure() {
      bindFactory(RuntimeInfoTestInjector.class).to(RuntimeInfo.class);
    }
  }

  static class RuntimeInfoTestInjector implements Factory<RuntimeInfo> {

    public RuntimeInfoTestInjector() {
    }

    @Singleton
    @Override
    public RuntimeInfo provide() {
      confDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
      confDir.mkdirs();
      Assert.assertTrue("Could not create: " + confDir, confDir.isDirectory());
      RuntimeInfo mock = Mockito.mock(RuntimeInfo.class);
      Mockito.when(mock.getConfigDir()).thenReturn(confDir.getAbsolutePath());
      return mock;
    }

    @Override
    public void dispose(RuntimeInfo runtimeInfo) {

    }
  }
}

