/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.preview.async.dagger;

import com.streamsets.dc.execution.Previewer;
import com.streamsets.dc.execution.executor.ExecutorModule;
import com.streamsets.dc.execution.preview.async.AsyncPreviewer;
import com.streamsets.dc.execution.preview.sync.SyncPreviewer;
import com.streamsets.dc.execution.preview.sync.dagger.SyncPreviewerModule;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;

/**
 * Provides instances of AsyncPreviewer
 */
@Module(injects = Previewer.class, library = true, includes = {SyncPreviewerModule.class, ExecutorModule.class})
public class AsyncPreviewerModule {

  @Provides
  public Previewer providePreviewer(SyncPreviewer syncPreviewer,
                                    @Named("previewExecutor") SafeScheduledExecutorService previewExecutor) {
    return new AsyncPreviewer(syncPreviewer, previewExecutor);
  }
}
