/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.runner.preview;

import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.task.TaskWrapper;

import java.util.Collections;
import java.util.List;

public class PreviewStageLibraryTask extends TaskWrapper implements StageLibraryTask {
  public static final String LIBRARY = ":system:";
  public static final String NAME = ":plug:";
  public static final String VERSION = "1.0.0";

  private final StageLibraryTask library;

  public PreviewStageLibraryTask(StageLibraryTask library) {
    super(library);
    this.library = library;
  }

  @Override
  public List<StageDefinition> getStages() {
    return library.getStages();
  }

  @Override
  public StageDefinition getStage(String library, String name, String version) {
    StageDefinition def = null;
    if (LIBRARY.equals(library) && NAME.equals(name) && VERSION.equals(VERSION)) {
      def = new StageDefinition(PreviewPlugTarget.class.getName(), NAME, VERSION, "previewPlug", "Preview Plug",
          StageType.TARGET, Collections.EMPTY_LIST, StageDef.OnError.DROP_BATCH, null/*raw source definition*/, "");
      def.setLibrary(LIBRARY, getClass().getClassLoader());
    } else {
      def = this.library.getStage(library, name, version);
    }
    return def;
  }

}
