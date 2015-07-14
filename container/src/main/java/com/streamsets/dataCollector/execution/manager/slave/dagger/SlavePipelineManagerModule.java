/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.manager.slave.dagger;

import com.streamsets.dataCollector.execution.executor.ExecutorModule;
import com.streamsets.dataCollector.execution.manager.slave.SlavePipelineManager;
import com.streamsets.dataCollector.execution.preview.common.dagger.PreviewerProviderModule;
import com.streamsets.dataCollector.execution.runner.provider.dagger.SlaveRunnerProviderModule;
import com.streamsets.dataCollector.execution.snapshot.cache.dagger.CacheSnapshotStoreModule;
import com.streamsets.dataCollector.execution.store.SlavePipelineStateStoreModule;
import com.streamsets.pipeline.store.SlavePipelineStoreModule;
import dagger.Module;

/**
 * Provides a singleton instance of Manager.
 */
@Module(library = true, injects = {SlavePipelineManager.class},
  includes = {SlavePipelineStateStoreModule.class, SlavePipelineStoreModule.class, ExecutorModule.class,
    PreviewerProviderModule.class, SlaveRunnerProviderModule.class, CacheSnapshotStoreModule.class})
public class SlavePipelineManagerModule {

}
