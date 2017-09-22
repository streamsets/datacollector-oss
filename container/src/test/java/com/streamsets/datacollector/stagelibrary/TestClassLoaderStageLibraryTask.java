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
package com.streamsets.datacollector.stagelibrary;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.config.StageLibraryDefinition;
import com.streamsets.datacollector.el.ElConstantDefinition;
import com.streamsets.datacollector.el.ElFunctionDefinition;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;

import com.streamsets.pipeline.ApplicationPackage;
import com.streamsets.pipeline.SDCClassLoader;
import com.streamsets.pipeline.SystemPackage;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public class TestClassLoaderStageLibraryTask {

  @Test
  public void testValidateStageVersionsOK() {
    ClassLoaderStageLibraryTask library = new ClassLoaderStageLibraryTask(null, new Configuration());

    StageDefinition stage1 = Mockito.mock(StageDefinition.class);
    Mockito.when(stage1.getName()).thenReturn("s");
    Mockito.when(stage1.getVersion()).thenReturn(1);

    StageDefinition stage2 = Mockito.mock(StageDefinition.class);
    Mockito.when(stage2.getName()).thenReturn("s");
    Mockito.when(stage2.getVersion()).thenReturn(1);

    List<StageDefinition> stages = ImmutableList.of(stage1, stage2);

    library.validateStageVersions(stages);
  }

  @Test(expected = RuntimeException.class)
  public void testValidateStageVersionsFail() {
    ClassLoaderStageLibraryTask library = new ClassLoaderStageLibraryTask(null, new Configuration());

    StageDefinition stage1 = Mockito.mock(StageDefinition.class);
    Mockito.when(stage1.getName()).thenReturn("s");
    Mockito.when(stage1.getVersion()).thenReturn(1);

    StageDefinition stage2 = Mockito.mock(StageDefinition.class);
    Mockito.when(stage2.getName()).thenReturn("s");
    Mockito.when(stage2.getVersion()).thenReturn(2);

    List<StageDefinition> stages = ImmutableList.of(stage1, stage2);

    library.validateStageVersions(stages);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAutoELs() {
    File configDir = new File("target", UUID.randomUUID().toString()).getAbsoluteFile();
    ClassLoader cl = new SDCClassLoader("library", "lib", Collections.<URL>emptyList(), getClass().getClassLoader(),
                                        new String[0], new SystemPackage(new String[0]),
                                        new ApplicationPackage(new TreeSet<String>()), false, false, false);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getConfigDir()).thenReturn(configDir.getAbsolutePath());
    Mockito.when(runtimeInfo.getStageLibraryClassLoaders()).thenReturn((List) ImmutableList.of(cl));

    ClassLoaderStageLibraryTask library = new ClassLoaderStageLibraryTask(runtimeInfo, new Configuration());
    library.initTask();

    Assert.assertEquals(1, library.getStages().size());
    StageDefinition sDef = library.getStages().get(0);
    ConfigDefinition cDef = sDef.getConfigDefinition("foo");
    System.out.println(cDef.getElFunctionDefinitions());
    System.out.println(cDef.getElConstantDefinitions());
    boolean foundAutoF = false;
    boolean foundAutoC = false;
    for (ElFunctionDefinition fDef : cDef.getElFunctionDefinitions()) {
      foundAutoF = fDef.getName().equals("foo:bar");
      if (foundAutoF) {
        break;
      }
    }
    Assert.assertTrue(foundAutoF);
    for (ElConstantDefinition fDef : cDef.getElConstantDefinitions()) {
      foundAutoC = fDef.getName().equals("FOOBAR");
      if (foundAutoC) {
        break;
      }
    }
    Assert.assertTrue(foundAutoC);
  }

  @Test
  public void testIgnoreStages() throws Exception {
    ClassLoaderStageLibraryTask library = new ClassLoaderStageLibraryTask(null, new Configuration());

    StageLibraryDefinition libDef = Mockito.mock(StageLibraryDefinition.class);
    Mockito.when(libDef.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
    Set<String> ignoreList = library.loadIgnoreStagesList(libDef);

    Assert.assertEquals(ImmutableSet.of("foo", "bar"), ignoreList);

    List<String> stageList = ImmutableList.of("a", "b", "c");
    Assert.assertEquals(stageList, library.removeIgnoreStagesFromList(libDef, stageList));

    List<String> stageListWithIgnores = ImmutableList.of("a", "bar", "b", "c", "foo");
    Assert.assertEquals(stageList, library.removeIgnoreStagesFromList(libDef, stageListWithIgnores));
  }

  @Test(expected = RuntimeException.class)
  public void testDuplicateServices() throws Exception {
    ClassLoaderStageLibraryTask library = new ClassLoaderStageLibraryTask(null, new Configuration());

    ServiceDefinition definition = Mockito.mock(ServiceDefinition.class);
    Mockito.when(definition.getProvides()).thenReturn(Runnable.class);

    library.validateServices(Collections.emptyList(), ImmutableList.of(definition, definition));
  }
}
