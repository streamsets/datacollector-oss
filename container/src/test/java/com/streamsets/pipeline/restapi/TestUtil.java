/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import org.glassfish.hk2.api.Factory;
import org.mockito.Mockito;

import javax.inject.Singleton;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestUtil {

  /**
   * Mock source implementation
   */
  public static class TSource extends BaseSource {
    public boolean inited;
    public boolean destroyed;

    @Override
    protected void init() throws StageException {
      inited = true;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      return null;
    }
  }

  /**
   * Mock target implementation
   */
  public static class TTarget extends BaseTarget {
    public boolean inited;
    public boolean destroyed;

    @Override
    protected void init() throws StageException {
      inited = true;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }
    @Override
    public void write(Batch batch) throws StageException {
    }
  }

  @SuppressWarnings("unchecked")
  /**
   *
   * @return Mock stage library implementation
   */
  public static StageLibraryTask createMockStageLibrary() {
    StageLibraryTask lib = Mockito.mock(StageLibraryTask.class);
    List<ConfigDefinition> configDefs = new ArrayList<>();
    ConfigDefinition configDef = new ConfigDefinition("string", ConfigDef.Type.STRING, "l1", "d1", "--", true, "g",
        "stringVar", null, "", new String[] {});
    configDefs.add(configDef);
    configDef = new ConfigDefinition("int", ConfigDef.Type.INTEGER, "l2", "d2", "-1", true, "g", "intVar", null, "",
      new String[] {});
    configDefs.add(configDef);
    configDef = new ConfigDefinition("long", ConfigDef.Type.INTEGER, "l3", "d3", "-2", true, "g", "longVar", null, "",
      new String[] {});
    configDefs.add(configDef);
    configDef = new ConfigDefinition("boolean", ConfigDef.Type.BOOLEAN, "l4", "d4", "false", true, "g", "booleanVar",
      null, "", new String[] {});
    configDefs.add(configDef);
    StageDefinition sourceDef = new StageDefinition(
        TSource.class.getName(), "source", "1.0.0", "label", "description",
        StageType.SOURCE, configDefs, null/*raw source definition*/, "");
    sourceDef.setLibrary("library", Thread.currentThread().getContextClassLoader());
    StageDefinition targetDef = new StageDefinition(
        TTarget.class.getName(), "target", "1.0.0", "label", "description",
        StageType.TARGET, Collections.EMPTY_LIST, null/*raw source definition*/,
        "TargetIcon.svg");
    targetDef.setLibrary("library", Thread.currentThread().getContextClassLoader());
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("source"), Mockito.eq("1.0.0"))).thenReturn(sourceDef);
    Mockito.when(lib.getStage(Mockito.eq("library"), Mockito.eq("target"), Mockito.eq("1.0.0"))).thenReturn(targetDef);

    List<StageDefinition> stages = new ArrayList<>(2);
    stages.add(sourceDef);
    stages.add(targetDef);
    Mockito.when(lib.getStages()).thenReturn(stages);
    return lib;
  }

  public static class StageLibraryTestInjector implements Factory<StageLibraryTask> {

    public StageLibraryTestInjector() {
    }

    @Singleton
    @Override
    public StageLibraryTask provide() {
      return createMockStageLibrary();
    }

    @Override
    public void dispose(StageLibraryTask stageLibrary) {
    }
  }

  public static class URITestInjector implements Factory<URI> {
    @Override
    public URI provide() {
      try {
        return new URI("URIInjector");
      } catch (URISyntaxException e) {
        e.printStackTrace();
        return null;
      }
    }

    @Override
    public void dispose(URI uri) {
    }

  }

  public static class PrincipalTestInjector implements Factory<Principal> {

    @Override
    public Principal provide() {
      return new Principal() {
        @Override
        public String getName() {
          return "nobody";
        }
      };
    }

    @Override
    public void dispose(Principal principal) {
    }

  }
}
