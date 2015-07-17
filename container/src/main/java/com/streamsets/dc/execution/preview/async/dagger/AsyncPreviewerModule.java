/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.preview.async.dagger;

import com.streamsets.dc.execution.Previewer;
import com.streamsets.dc.execution.PreviewerListener;
import com.streamsets.dc.execution.preview.async.AsyncPreviewer;
import com.streamsets.dc.execution.preview.sync.SyncPreviewer;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Named;

/**
 * Provides instances of SyncPreviewer.
 */
@Module(injects = Previewer.class, library = true, complete = false)
public class AsyncPreviewerModule {

  private final String id;
  private final String name;
  private final String rev;
  private final ObjectGraph objectGraph;
  private final PreviewerListener previewerListener;

  public AsyncPreviewerModule(String id, String name, String rev, PreviewerListener previewerListener,
                              ObjectGraph objectGraph) {
    this.id = id;
    this.name = name;
    this.rev = rev;
    this.objectGraph = objectGraph;
    this.previewerListener = previewerListener;
  }


  @Provides
  public SyncPreviewer providePreviewer() {
    return new SyncPreviewer(id, name, rev, previewerListener, objectGraph);
  }

  @Provides
  public Previewer provideAsyncPreviewer(SyncPreviewer previewer,
                                         @Named("previewExecutor") SafeScheduledExecutorService asyncExecutor) {
    return new AsyncPreviewer(previewer, asyncExecutor);
  }
}
