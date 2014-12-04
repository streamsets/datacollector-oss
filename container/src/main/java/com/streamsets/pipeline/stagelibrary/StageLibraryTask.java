/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stagelibrary;

import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.task.Task;

import java.util.List;
import java.util.Locale;

public interface StageLibraryTask extends Task {

  public List<StageDefinition> getStages();

  public StageDefinition getStage(String library, String name, String version);

}
