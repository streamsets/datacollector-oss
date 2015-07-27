/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.manager.standalone.dagger;

import com.streamsets.datacollector.execution.executor.ExecutorModule;
import com.streamsets.datacollector.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.datacollector.execution.preview.common.dagger.PreviewerProviderModule;
import com.streamsets.datacollector.execution.runner.provider.dagger.StandaloneAndClusterRunnerProviderModule;
import com.streamsets.datacollector.execution.snapshot.cache.dagger.CacheSnapshotStoreModule;
import com.streamsets.datacollector.execution.store.CachePipelineStateStoreModule;
import com.streamsets.datacollector.store.CachePipelineStoreModule;

import dagger.Module;

/**
 * Provides a singleton instance of Manager.
 */
@Module(library = true, injects = {StandaloneAndClusterPipelineManager.class},
  includes = {CachePipelineStateStoreModule.class, CachePipelineStoreModule.class, ExecutorModule.class,
    PreviewerProviderModule.class, StandaloneAndClusterRunnerProviderModule.class, CacheSnapshotStoreModule.class})
public class StandalonePipelineManagerModule {

}
