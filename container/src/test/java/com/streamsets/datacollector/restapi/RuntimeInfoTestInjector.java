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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import org.glassfish.hk2.api.Factory;
import org.junit.Assert;
import org.mockito.Mockito;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.mockito.Mockito.mock;

public class RuntimeInfoTestInjector implements Factory<RuntimeInfo> {

  public static File confDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
  static {
    confDir.mkdirs();    try {
      Configuration.setFileRefsBaseDir(confDir);
      new File(confDir, "sdc.properties").createNewFile();
      new File(confDir, "dpm.properties").createNewFile();
    } catch (IOException e ) {

    }
    Assert.assertTrue("Could not create: " + confDir, confDir.isDirectory());
  }

  public RuntimeInfoTestInjector() {
  }

  @Singleton
  @Override
  public RuntimeInfo provide() {
    RuntimeInfo mock = mock(RuntimeInfo.class);
    Mockito.when(mock.getConfigDir()).thenReturn(confDir.getAbsolutePath());
    return mock;
  }

  @Override
  public void dispose(RuntimeInfo runtimeInfo) {

  }
}
