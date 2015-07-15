/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.preview.common.dagger;

import com.streamsets.dc.execution.executor.ExecutorModule;
import com.streamsets.dc.execution.manager.PreviewerProvider;
import com.streamsets.dc.execution.preview.common.PreviewerProviderImpl;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

/**
 * Provides a singleton instance of PreviewerProvider.
 */
@Module(injects = PreviewerProvider.class, includes = {ExecutorModule.class})
public class PreviewerProviderModule {

  @Provides @Singleton
  public PreviewerProvider providePreviewerProvider(PreviewerProviderImpl previewerProvider) {
    return previewerProvider;
  }

}
