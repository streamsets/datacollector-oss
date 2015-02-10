/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.prodmanager.PipelineManagerException;
import com.streamsets.pipeline.prodmanager.PipelineState;
import com.streamsets.pipeline.prodmanager.ProductionPipelineManagerTask;
import com.streamsets.pipeline.prodmanager.State;
import com.streamsets.pipeline.record.RecordImpl;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.snapshotstore.SnapshotStatus;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
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

  private static final String PIPELINE_NAME = "myPipeline";
  private static final String PIPELINE_REV = "2.0";
  private static final String DEFAULT_PIPELINE_REV = "0";

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
        "stringVar", null, "", new String[] {}, 0);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("int", ConfigDef.Type.INTEGER, "l2", "d2", "-1", true, "g", "intVar", null, "",
      new String[] {}, 0);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("long", ConfigDef.Type.INTEGER, "l3", "d3", "-2", true, "g", "longVar", null, "",
      new String[] {}, 0);
    configDefs.add(configDef);
    configDef = new ConfigDefinition("boolean", ConfigDef.Type.BOOLEAN, "l4", "d4", "false", true, "g", "booleanVar",
      null, "", new String[] {}, 0);
    configDefs.add(configDef);
    StageDefinition sourceDef = new StageDefinition(
        TSource.class.getName(), "source", "1.0.0", "label", "description",
        StageType.SOURCE, false, true, configDefs, null/*raw source definition*/, "", null, false ,1, null);
    sourceDef.setLibrary("library", "", Thread.currentThread().getContextClassLoader());
    StageDefinition targetDef = new StageDefinition(
        TTarget.class.getName(), "target", "1.0.0", "label", "description",
        StageType.TARGET, false, true, Collections.<ConfigDefinition>emptyList(), null/*raw source definition*/,
        "TargetIcon.svg", null, false, 0, null);
    targetDef.setLibrary("library", "", Thread.currentThread().getContextClassLoader());
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

  static class PipelineManagerTestInjector implements Factory<ProductionPipelineManagerTask> {

    public PipelineManagerTestInjector() {
    }

    @Singleton
    @Override
    public ProductionPipelineManagerTask provide() {

      ProductionPipelineManagerTask pipelineManager = Mockito.mock(ProductionPipelineManagerTask.class);
      try {
        Mockito.when(pipelineManager.startPipeline(PIPELINE_NAME, PIPELINE_REV)).thenReturn(new PipelineState(
          PIPELINE_NAME, "2.0", State.RUNNING, "The pipeline is now running", System.currentTimeMillis()));
      } catch (PipelineManagerException | StageException | PipelineRuntimeException | PipelineStoreException e) {
        e.printStackTrace();
      }

      try {
        Mockito.when(pipelineManager.stopPipeline(false)).thenReturn(
          new PipelineState(PIPELINE_NAME, PIPELINE_REV, State.STOPPED, "The pipeline is not running", System.currentTimeMillis()));
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      Mockito.when(pipelineManager.getPipelineState()).thenReturn(new PipelineState(PIPELINE_NAME, PIPELINE_REV, State.STOPPED
        , "Pipeline is not running", System.currentTimeMillis()));

      try {
        Mockito.when(pipelineManager.getSnapshot(PIPELINE_NAME, DEFAULT_PIPELINE_REV))
          .thenReturn(getClass().getClassLoader().getResourceAsStream("snapshot.json"))
          .thenReturn(null);
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      Mockito.when(pipelineManager.getSnapshotStatus()).thenReturn(new SnapshotStatus(false, true));

      Mockito.when(pipelineManager.getMetrics()).thenReturn(new MetricRegistry());

      List<PipelineState> states = new ArrayList<>();
      states.add(new PipelineState(PIPELINE_NAME, "1", State.STOPPED, "", System.currentTimeMillis()));
      states.add(new PipelineState(PIPELINE_NAME, "1", State.RUNNING, "", System.currentTimeMillis()));
      states.add(new PipelineState(PIPELINE_NAME, "1", State.STOPPED, "", System.currentTimeMillis()));
      try {
        Mockito.when(pipelineManager.getHistory(PIPELINE_NAME, DEFAULT_PIPELINE_REV, false)).thenReturn(states);
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      Mockito.doNothing().when(pipelineManager).deleteSnapshot(PIPELINE_NAME, PIPELINE_REV);
      try {
        Mockito.doNothing().when(pipelineManager).deleteErrors(PIPELINE_NAME, "1");
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      try {
        Mockito.when(pipelineManager.getErrors(PIPELINE_NAME, "0"))
          .thenReturn(getClass().getClassLoader().getResourceAsStream("snapshot.json"))
          .thenReturn(null);
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      Record r = new RecordImpl("a", "b", "c".getBytes(), "d");
      try {
        Mockito.when(pipelineManager.getErrorRecords("myProcessorStage", 100)).thenReturn(
          ImmutableList.of(r));
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      ErrorMessage em = new ErrorMessage("a", "b", 2L);
      try {
        Mockito.when(pipelineManager.getErrorMessages("myProcessorStage")).thenReturn(
          ImmutableList.of(em));
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      try {
        Mockito.doNothing().when(pipelineManager).resetOffset(PIPELINE_NAME, PIPELINE_REV);
      } catch (PipelineManagerException e) {
        e.printStackTrace();
      }

      return pipelineManager;
    }

    @Override
    public void dispose(ProductionPipelineManagerTask pipelineManagerTask) {
    }
  }
}
