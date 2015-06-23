/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stagelibrary;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.main.RuntimeInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class TestClassLoaderStageLibraryTask {

  @StageDef(label = "L", version = "1.0.0")
  public static class DummyStage extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLibraryLoad() throws Exception {
    RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);
    Mockito.when(runtimeInfo.getStageLibraryClassLoaders()).thenReturn((List)Arrays.asList(getClass().getClassLoader()));
    StageLibraryTask library = new ClassLoaderStageLibraryTask(runtimeInfo);
    try {
      library.init();
      library.run();
      Assert.assertNotNull(library.getStage("default", DummyStage.class.getName().replace(".", "_").replace("$", "_"),
                                            "1.0.0"));
    } finally {
      library.stop();
    }
  }
}
