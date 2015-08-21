/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.stagelibrary;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.config.ConfigDefinition;
import com.streamsets.datacollector.config.StageDefinition;
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

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

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
  public void testAutoELs() {
    ClassLoader cl = new SDCClassLoader("library", "lib", Collections.<URL>emptyList(), getClass().getClassLoader(),
                                        new String[0], new SystemPackage(new String[0]),
                                        new ApplicationPackage(new TreeSet<String>()), false, false);
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
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

}
