/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.preview.sync.dagger;

import com.streamsets.dataCollector.execution.PreviewerListener;
import com.streamsets.dataCollector.execution.preview.sync.SyncPreviewer;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.CachePipelineStoreModule;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.Configuration;
import dagger.Module;
import dagger.Provides;

/**
 * Provides instances of SyncPreviewer.
 */
@Module(injects = SyncPreviewer.class, library = true, includes = {CachePipelineStoreModule.class})
public class SyncPreviewerModule {

  private final String id;
  private final String name;
  private final String rev;
  private final PreviewerListener previewerListener;

  public SyncPreviewerModule(String id, String name, String rev, PreviewerListener previewerListener) {
    this.id = id;
    this.name = name;
    this.rev = rev;
    this.previewerListener = previewerListener;
  }

  @Provides
  public SyncPreviewer providePreviewer(Configuration configuration,
                                    StageLibraryTask stageLibraryTask, PipelineStoreTask pipelineStoreTask,
                                    RuntimeInfo runtimeInfo) {
    return new SyncPreviewer(id, name, rev, previewerListener, configuration, stageLibraryTask, pipelineStoreTask,
      runtimeInfo);
  }
}
