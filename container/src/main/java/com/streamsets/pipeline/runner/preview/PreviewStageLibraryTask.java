/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.preview;

import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.StageUpgrader;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageLibraryDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.task.TaskWrapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PreviewStageLibraryTask extends TaskWrapper implements StageLibraryTask {
  public static final String LIBRARY = ":system:";
  public static final String NAME = ":plug:";
  public static final int VERSION = 1;

  private static final StageLibraryDefinition PREVIEW_LIB = new StageLibraryDefinition(
      PreviewStageLibraryTask.class.getClassLoader(), LIBRARY, "Preview", new Properties());

  private static final StageDefinition PLUG_STAGE =  new StageDefinition(PREVIEW_LIB, false, PreviewPlugTarget.class,
      NAME, VERSION, "previewPlug", "Preview Plug", StageType.TARGET, false, false, false, Collections.<ConfigDefinition>emptyList(),
      null/*raw source definition*/, "", null, false, 0, null, Arrays.asList(ExecutionMode.STANDALONE), false,
      new StageUpgrader.Default());

  private final StageLibraryTask library;

  public PreviewStageLibraryTask(StageLibraryTask library) {
    super(library);
    this.library = library;
  }

  @Override
  public PipelineDefinition getPipeline() {
    return library.getPipeline();
  }

  @Override
  public List<StageDefinition> getStages() {
    return library.getStages();
  }

  @Override
  public StageDefinition getStage(String library, String name, boolean forExecution) {
    StageDefinition def;
    if (LIBRARY.equals(library) && NAME.equals(name)) {
      def = PLUG_STAGE;
    } else {
      def = this.library.getStage(library, name, forExecution);
    }
    return def;
  }

  @Override
  public Map<String, String> getLibraryNameAliases() {
    return library.getLibraryNameAliases();
  }

  @Override
  public void releaseStageClassLoader(ClassLoader classLoader) {
    if (classLoader != PLUG_STAGE.getClass().getClassLoader()) {
      library.releaseStageClassLoader(classLoader);
    }
  }

}
