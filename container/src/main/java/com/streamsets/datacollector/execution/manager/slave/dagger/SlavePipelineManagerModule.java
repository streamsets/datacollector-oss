/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.manager.slave.dagger;

import com.streamsets.datacollector.execution.executor.ExecutorModule;
import com.streamsets.datacollector.execution.manager.slave.SlavePipelineManager;
import com.streamsets.datacollector.execution.preview.common.dagger.PreviewerProviderModule;
import com.streamsets.datacollector.execution.runner.provider.dagger.SlaveRunnerProviderModule;
import com.streamsets.datacollector.execution.snapshot.cache.dagger.CacheSnapshotStoreModule;
import com.streamsets.datacollector.execution.store.SlavePipelineStateStoreModule;
import com.streamsets.datacollector.store.SlavePipelineStoreModule;

import dagger.Module;

/**
 * Provides a singleton instance of Manager.
 */
@Module(library = true, injects = {SlavePipelineManager.class},
  includes = {SlavePipelineStateStoreModule.class, SlavePipelineStoreModule.class, ExecutorModule.class,
    PreviewerProviderModule.class, SlaveRunnerProviderModule.class, CacheSnapshotStoreModule.class})
public class SlavePipelineManagerModule {

}
