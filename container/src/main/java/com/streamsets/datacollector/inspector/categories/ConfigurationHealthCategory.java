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
package com.streamsets.datacollector.inspector.categories;

import com.streamsets.datacollector.execution.common.ExecutorConstants;
import com.streamsets.datacollector.execution.executor.ExecutorModule;
import com.streamsets.datacollector.execution.runner.common.Constants;
import com.streamsets.datacollector.inspector.HealthCategory;
import com.streamsets.datacollector.inspector.model.HealthCategoryResult;
import com.streamsets.datacollector.inspector.model.HealthCheck;
import com.streamsets.datacollector.restapi.PreviewResource;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.stagelibrary.ClassLoaderStageLibraryTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.websockets.LogMessageWebSocket;

public class ConfigurationHealthCategory implements HealthCategory {
  @Override
  public String getName() {
    return "Data Collector Configuration";
  }

  @Override
  public HealthCategoryResult inspectHealth(Context context) {
    HealthCategoryResult.Builder builder = new HealthCategoryResult.Builder(this);
    // Our main configuration
    Configuration configuration = context.getConfiguration();


    int maxBatchSizeForPreview = configuration.get(PreviewResource.MAX_BATCH_SIZE_KEY, PreviewResource.MAX_BATCH_SIZE_DEFAULT);
    builder.addHealthCheck("Max Batch Size for Preview", HealthCheck.Severity.smallerIsBetter(maxBatchSizeForPreview, 2_000, 5_000))
        .withValue(maxBatchSizeForPreview)
        .withDescription("Maximal number of records that can be created in a single batch while in preview. Controlled by {} in sdc.properties", PreviewResource.MAX_BATCH_SIZE_KEY);

    int maxBatchSize = configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT);
    builder.addHealthCheck("Max Batch Size", HealthCheck.Severity.smallerIsBetter(maxBatchSize, 50_000, 100_000))
        .withValue(maxBatchSize)
        .withDescription("Maximal number of records that origin should create in a single batch while running a pipeline. Controlled by {} in sdc.properties", Constants.MAX_BATCH_SIZE_KEY);

    int maxErrorRecordsPerStage = configuration.get(Constants.MAX_ERROR_RECORDS_PER_STAGE_KEY, Constants.MAX_ERROR_RECORDS_PER_STAGE_DEFAULT);
    builder.addHealthCheck("Max Error Records Per Stage", HealthCheck.Severity.smallerIsBetter(maxErrorRecordsPerStage, 101, 1_000))
        .withValue(maxErrorRecordsPerStage)
        .withDescription("Maximal number of error records that should be kept in memory while pipeline is running per each stage of each running pipeline. Controlled by {} in sdc.properties", Constants.MAX_ERROR_RECORDS_PER_STAGE_KEY);

    int maxPipelineErrors = configuration.get(Constants.MAX_PIPELINE_ERRORS_KEY, Constants.MAX_PIPELINE_ERRORS_DEFAULT);
    builder.addHealthCheck("Max Pipeline Errors", HealthCheck.Severity.smallerIsBetter(maxPipelineErrors, 101, 1_000))
        .withValue(maxPipelineErrors)
        .withDescription("Maximal number of errors that should be kept in memory while pipeline is running per running each pipeline. Controlled by {} in sdc.properties", Constants.MAX_PIPELINE_ERRORS_KEY);

    int maxLogTailers = configuration.get(LogMessageWebSocket.MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY, LogMessageWebSocket.MAX_LOGTAIL_CONCURRENT_REQUESTS_DEFAULT);
    builder.addHealthCheck("Max Log Tailers", HealthCheck.Severity.smallerIsBetter(maxLogTailers, 6, 20))
        .withValue(maxLogTailers)
        .withDescription("Maximal number of concurrent sessions that can be tailing logs in Data Collector Logs view. Controlled by {} in sdc.properties", LogMessageWebSocket.MAX_LOGTAIL_CONCURRENT_REQUESTS_KEY);

    int maxPrivateStageLoaders = configuration.get(ClassLoaderStageLibraryTask.MAX_PRIVATE_STAGE_CLASS_LOADERS_KEY, ClassLoaderStageLibraryTask.MAX_PRIVATE_STAGE_CLASS_LOADERS_DEFAULT);
    builder.addHealthCheck("Max Private ClassLoader", HealthCheck.Severity.smallerIsBetter(maxPrivateStageLoaders, 51, 150))
        .withValue(maxLogTailers)
        .withDescription("Maximal number of stages that requires separate instance of a classloader that can be running at a time. Controlled by {} in sdc.properties", ClassLoaderStageLibraryTask.MAX_PRIVATE_STAGE_CLASS_LOADERS_KEY);

    int maxRunnerSize = ExecutorModule.getRunnerSize(configuration);
    builder.addHealthCheck("Max Runner Size", HealthCheck.Severity.smallerIsBetter(maxRunnerSize, 100, 500))
        .withValue(maxRunnerSize)
        .withDescription("Maximal size of thread pool that is responsible for running pipelines. Controlled by {} in sdc.properties", ExecutorConstants.RUNNER_THREAD_POOL_SIZE_KEY);

    int maxPipelineRunnerSize = configuration.get(Pipeline.MAX_RUNNERS_CONFIG_KEY, Pipeline.MAX_RUNNERS_DEFAULT);
    builder.addHealthCheck("Max Pipeline Runner Size", HealthCheck.Severity.smallerIsBetter(maxPipelineRunnerSize, 51, 100))
        .withValue(maxPipelineRunnerSize)
        .withDescription("Maximal number of Pipeline Runners a single multi threaded pipeline can create. Controlled by {} in sdc.properties", Pipeline.MAX_RUNNERS_CONFIG_KEY);

    return builder.build();
  }

}
