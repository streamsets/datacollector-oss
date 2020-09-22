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
package com.streamsets.datacollector.activation;

import com.streamsets.datacollector.main.RuntimeInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;

public class TestActivationLoader {
  private File metaInfDir;
  private File serviceFile;

  @Before
  public void setup() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    URL url = classLoader.getResource("testclasses.txt");
    String path = url.getPath();
    Assert.assertNotNull(path);
    File file = new File(path);
    metaInfDir = new File(file.getParent(), "META-INF/services");
    metaInfDir.mkdirs();
    Assert.assertTrue(metaInfDir.exists());
    serviceFile = new File(metaInfDir, Activation.class.getName());
    serviceFile.delete();
    Assert.assertFalse(serviceFile.exists());
  }

  @After
  public void cleanup() {
    serviceFile.delete();
    Assert.assertFalse(serviceFile.exists());
  }

  @Test
  public void testNoActivationDefined() {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Activation activation = new ActivationLoader(runtimeInfo).getActivation();
    Assert.assertTrue(activation instanceof NopActivation);
  }

  @Test
  public void testActivationDefined() throws IOException {
    try (PrintWriter writer = new PrintWriter(new FileWriter(serviceFile))) {
      writer.println(DummyActivation.class.getName());
    }
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Activation activation = new ActivationLoader(runtimeInfo).getActivation();
    Assert.assertNotNull(activation);
    Assert.assertEquals(runtimeInfo, ((DummyActivation)activation).getRuntimeInfo());
  }

  @Test(expected = RuntimeException.class)
  public void testMultipleActivationsDefined() throws IOException {
    try (PrintWriter writer = new PrintWriter(new FileWriter(serviceFile))) {
      writer.println(DummyActivation.class.getName());
      writer.println(Dummy2Activation.class.getName());
    }
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    new ActivationLoader(runtimeInfo).getActivation();
  }

}
