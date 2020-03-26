/*
 * Copyright 2020 StreamSets Inc.
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
import com.streamsets.pipeline.SDCClassLoader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class TestWhitelistActivation {

  @Test
  public void testWhitelistActivation() {
    Activation activation = Mockito.mock(Activation.class);
    Activation.Info info = Mockito.mock(Activation.Info.class);
    Mockito.when(activation.getInfo()).thenReturn(info);
    Mockito.when(info.isValid()).thenReturn(false);
    WhitelistActivation whitelisted = new WhitelistActivation(activation);

    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    List<SDCClassLoader> basicClassLoaders = createMockClassLoaders(
        "streamsets-datacollector-basic-lib",
        "streamsets-datacollector-dev-lib"
    );
    Mockito.doReturn(basicClassLoaders).when(runtimeInfo).getStageLibraryClassLoaders();
    whitelisted.init(runtimeInfo);
    Assert.assertTrue(whitelisted.getInfo().isValid());

    List<SDCClassLoader> classLoaders = createMockClassLoaders(
        "streamsets-datacollector-basic-lib",
        "streamsets-datacollector-dev-lib",
        "streamsets-datacollector-jdbc-lib"
    );
    Mockito.doReturn(classLoaders).when(runtimeInfo).getStageLibraryClassLoaders();
    whitelisted.init(runtimeInfo);
    Assert.assertFalse(whitelisted.getInfo().isValid());
  }

  private List<SDCClassLoader> createMockClassLoaders(String... names) {
    List<SDCClassLoader> sdcClassLoaders = new ArrayList<>(names.length);
    for (String name : names) {
      SDCClassLoader classLoader = Mockito.mock(SDCClassLoader.class);
      Mockito.when(classLoader.getName()).thenReturn(name);
      sdcClassLoaders.add(classLoader);
    }
    return sdcClassLoaders;
  }
}
