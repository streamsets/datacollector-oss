/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.inspector.inspectors;

import com.streamsets.datacollector.execution.common.ExecutorConstants;
import com.streamsets.datacollector.execution.executor.ExecutorModule;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.inspector.HealthInspector;
import com.streamsets.datacollector.inspector.model.HealthInspectorResult;
import com.streamsets.datacollector.inspector.model.HealthInspectorEntry;
import com.streamsets.datacollector.restapi.PreviewResource;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.stagelibrary.ClassLoaderStageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.websockets.LogMessageWebSocket;
import com.streamsets.pipeline.api.impl.Utils;

public class ConfigurationInspector implements HealthInspector {
  @Override
  public String getName() {
    return "Data Collector Configuration";
  }

  @Override
  public HealthInspectorResult inspectHealth(Context context) {
    HealthInspectorResult.Builder builder = new HealthInspectorResult.Builder(this);
    // Our main configuration
    Configuration configuration = context.getConfiguration();

    createEntry(
        builder,
        configuration.get(PreviewResource.MAX_BATCH_SIZE_KEY, PreviewResource.MAX_BATCH_SIZE_DEFAULT),
        2_000,
        5_000,
        "Max Batch Size in Preview",
        PreviewResource.MAX_BATCH_SIZE_KEY
    );
    createEntry(
        builder,
        configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT),
        10_000,
        20_000,
        "Max Batch Size",
        Constants.MAX_BATCH_SIZE_KEY
    );
    createEntry(
        builder,
        configuration.get(Constants.MAX_ERROR_RECORDS_PER_STAGE_KEY, Constants.MAX_ERROR_RECORDS_PER_STAGE_DEFAULT),
        101,
        1000,
        "Max Error Records Per Stage",
        Constants.MAX_ERROR_RECORDS_PER_STAGE_KEY
    );
    createEntry(
        builder,
        configuration.get(Constants.MAX_PIPELINE_ERRORS_KEY, Constants.MAX_PIPELINE_ERRORS_DEFAULT),
        101,
        1000,
        "Max Pipeline Errors",
        Constants.MAX_PIPELINE_ERRORS_KEY
    );
    createEntry(
        builder,
        configuration.get(LogMessageWebSocket.MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY, LogMessageWebSocket.MAX_LOGTAIL_CONCURRENT_REQUESTS_DEFAULT),
        6,
        20,
        "Max SDC Log Tailers",
        LogMessageWebSocket.MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY
    );
    createEntry(
        builder,
        configuration.get(LogMessageWebSocket.MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY, LogMessageWebSocket.MAX_LOGTAIL_CONCURRENT_REQUESTS_DEFAULT),
        6,
        20,
        "Max SDC Log Tailers",
        LogMessageWebSocket.MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY
    );
    createEntry(
        builder,
        configuration.get(ClassLoaderStageLibraryTask.MAX_PRIVATE_STAGE_CLASS_LOADERS_KEY, ClassLoaderStageLibraryTask.MAX_PRIVATE_STAGE_CLASS_LOADERS_DEFAULT),
        51,
        150,
        "Max Private ClassLoaders",
        ClassLoaderStageLibraryTask.MAX_PRIVATE_STAGE_CLASS_LOADERS_KEY
    );
    createEntry(
        builder,
        ExecutorModule.getRunnerSize(configuration),
        100,
        500,
        "Number of runners",
        ExecutorConstants.RUNNER_THREAD_POOL_SIZE_KEY
    );
    createEntry(
        builder,
        configuration.get(Pipeline.MAX_RUNNERS_CONFIG_KEY, Pipeline.MAX_RUNNERS_DEFAULT),
        51,
        100,
        "Max number of pipeline runners",
        Pipeline.MAX_RUNNERS_CONFIG_KEY
    );

    return builder.build();
  }

  private void createEntry(HealthInspectorResult.Builder builder, long value, long greenToYellow, long yellowToRed, String name, String config) {
    builder.addEntry(name, HealthInspectorEntry.Severity.smallerIsBetter(value, greenToYellow, yellowToRed))
        .withValue(value)
        .withDescription(Utils.format("Configuration option {} in sdc.properties configuration file", config));
  }
}
