/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stagelibrary;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.util.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

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

}
