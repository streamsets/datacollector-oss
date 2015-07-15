/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.runner.common;

public class Constants {

  public static final String REFRESH_INTERVAL_PROPERTY = "ui.refresh.interval.ms";
  public static final int REFRESH_INTERVAL_PROPERTY_DEFAULT = 2000;
  public static final String CALLBACK_SERVER_URL_KEY = "callback.server.url";
  public static final String CALLBACK_SERVER_URL_DEFAULT = null;
  public static final String SDC_CLUSTER_TOKEN_KEY = "sdc.cluster.token";
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

  public static final String STOP_PIPELINE_MESSAGE = "Requested via REST API";
}
