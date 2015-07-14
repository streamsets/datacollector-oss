/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dataCollector.execution.manager.standalone.dagger;

import com.streamsets.dataCollector.execution.executor.ExecutorModule;
import com.streamsets.dataCollector.execution.manager.standalone.StandaloneAndClusterPipelineManager;
import com.streamsets.dataCollector.execution.preview.common.dagger.PreviewerProviderModule;
import com.streamsets.dataCollector.execution.runner.provider.dagger.StandaloneAndClusterRunnerProviderModule;
import com.streamsets.dataCollector.execution.snapshot.cache.dagger.CacheSnapshotStoreModule;
import com.streamsets.dataCollector.execution.store.CachePipelineStateStoreModule;
import com.streamsets.pipeline.store.CachePipelineStoreModule;
import dagger.Module;

/**
 * Provides a singleton instance of Manager.
 */
@Module(library = true, injects = {StandaloneAndClusterPipelineManager.class},
  includes = {CachePipelineStateStoreModule.class, CachePipelineStoreModule.class, ExecutorModule.class,
    PreviewerProviderModule.class, StandaloneAndClusterRunnerProviderModule.class, CacheSnapshotStoreModule.class})
public class StandalonePipelineManagerModule {

}
