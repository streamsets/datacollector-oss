/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

public class Constants {

  /*pipeline configuration properties*/
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
  public static final int SAMPLED_RECORDS_MAX_CACHE_SIZE_DEFAULT = 100;
  public static final String SAMPLED_RECORDS_MAX_CACHE_SIZE_KEY = "sampledRecordsToRetain.max.cache.size";
  public static final String MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_KEY = "max.observerRequest.offer.wait.time.ms";
  public static final int MAX_OBSERVER_REQUEST_OFFER_WAIT_TIME_MS_DEFAULT = 1000;
  public static final String RULES_CONFIG_LOADER_SLEEP_TIME_MS_KEY = "rulesConfigLoader.sleep.time.ms";
  public static final int RULES_CONFIG_LOADER_SLEEP_TIME_DEFAULT = 2000;

  public static final String STOP_PIPELINE_MESSAGE = "Requested via REST API";


}
