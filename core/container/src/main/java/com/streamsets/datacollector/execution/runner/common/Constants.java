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
package com.streamsets.datacollector.execution.runner.common;

public class Constants {

  public static final String REFRESH_INTERVAL_PROPERTY = "ui.refresh.interval.ms";
  public static final int REFRESH_INTERVAL_PROPERTY_DEFAULT = 2000;
  public static final String CALLBACK_SERVER_URL_KEY = "callback.server.url";
  public static final String CALLBACK_SERVER_URL_DEFAULT = null;
  public static final String MASTER_SDC_ID_SEPARATOR = ":";
  public static final String SDC_ID = "sdc.id";
  public static final String PIPELINE_CLUSTER_TOKEN_KEY = "pipeline.cluster.token";
  public static final String MAX_BATCH_SIZE_KEY = "production.maxBatchSize";
  public static final int MAX_BATCH_SIZE_DEFAULT = 1000;
  public static final String DELIVERY_GUARANTEE = "deliveryGuarantee";
  public static final String MAX_ERROR_FILE_SIZE_KEY = "production.maxErrorFileSize";
  public static final String MAX_ERROR_FILE_SIZE_DEFAULT = "1024MB";
  public static final String MAX_BACKUP_INDEX_KEY = "production.maxBackupIndex";
  public static final int MAX_BACKUP_INDEX_DEFAULT = 5;
  public static final String MAX_ERROR_RECORDS_PER_STAGE_KEY = "production.maxErrorRecordsPerStage";
  public static final int MAX_ERROR_RECORDS_PER_STAGE_DEFAULT = 100;
  public static final String MAX_PIPELINE_ERRORS_KEY = "production.maxPipelineErrors";
  public static final int MAX_PIPELINE_ERRORS_DEFAULT = 100;
  public static final String OBSERVER_QUEUE_SIZE_KEY = "observer.queue.size";
  public static final int OBSERVER_QUEUE_SIZE_DEFAULT = 100;
  public static final String SNAPSHOT_MAX_BATCH_SIZE_KEY = "snapshot.maxBatchSize";
  public static final int SNAPSHOT_MAX_BATCH_SIZE_DEFAULT = 10;
  public static final int SAMPLED_RECORDS_MAX_CACHE_SIZE_DEFAULT = 100;
  public static final String SAMPLED_RECORDS_MAX_CACHE_SIZE_KEY = "observer.sampled.records.cache.size";
  public static final String MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_KEY = "observer.queue.offer.max.wait.time.ms";
  public static final int MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_DEFAULT = 1000;
  public static final String MESOS_JAR_URL = "mesos.jar.url";
  public static final String STATS_AGGREGATOR_QUEUE_SIZE_KEY = "stats.queue.size";
  public static final int STATS_AGGREGATOR_QUEUE_SIZE_DEFAULT = 1000;
  public static final String MAX_STATS_REQUEST_OFFER_WAIT_TIME_MS_KEY = "stats.queue.offer.max.wait.time.ms";
  public static final int MAX_STATS_REQUEST_OFFER_WAIT_TIME_MS_DEFAULT = 0;
  public static final double MAX_HEAP_MEMORY_LIMIT_CONFIGURATION = 0.95;

  public static final String STOP_PIPELINE_MESSAGE = "Requested via REST API";

  private Constants() {}
}
