/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
                                StageType.TARGET, Collections.EMPTY_LIST, StageDef.OnError.DROP_BATCH, "");
      def.setLibrary(LIBRARY, getClass().getClassLoader());
    } else {
      def = this.library.getStage(library, name, version);
    }
    return def;
  }

}
