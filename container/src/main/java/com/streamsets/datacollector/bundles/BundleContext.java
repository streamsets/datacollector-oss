/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.bundles;

import com.streamsets.datacollector.blobstore.BlobStoreTask;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.usagestats.StatsCollector;
import com.streamsets.datacollector.util.Configuration;

/**
 * Shared context describing the bundle that is actively being created.
 *
 */
public interface BundleContext {

  /**
   * Fully provisioned SDC Configuration
   */
  public Configuration getConfiguration();

  /**
   * Returns BuildInfo structure for the data collector.
   */
  public BuildInfo getBuildInfo();

  /**
   * Returns RuntimeInfo structure for the data collector.
   */
  public RuntimeInfo getRuntimeInfo();

  /**
   * Returns pipeline store for current data collector.
   */
  public PipelineStoreTask getPipelineStore();

  /**
   * Returns pipeline state store for current data collector.
   */
  public PipelineStateStore getPipelineStateStore();

  /**
   * Returns snapshot store for current data collector.
   */
  public SnapshotStore getSnapshotStore();

  /**
   * Returns blob store instance.
   */
  public BlobStoreTask getBlobStore();

  /**
   * Returns stats collector task
   */
  public StatsCollector getStatsCollector();
}
