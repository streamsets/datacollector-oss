/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

public class Constants {

  /*Pipeline related constants*/
  public static final String DEFAULT_PIPELINE_NAME = "xyz";
  public static final String DEFAULT_PIPELINE_REVISION = "1.0";

  /*pipeline configuration properties*/
  public static final String MAX_BATCH_SIZE_KEY = "production.maxBatchSize";
  public static final int MAX_BATCH_SIZE_DEFAULT = 1000;
  public static final String DELIVERY_GUARANTEE = "deliveryGuarantee";
  public static final String MAX_ERROR_FILE_SIZE = "production.maxErrorFileSize";
  public static final String MAX_ERROR_FILE_SIZE_DEFAULT = "1024MB";
  public static final String MAX_BACKUP_INDEX = "production.maxBackupIndex";
  public static final int MAX_BACKUP_INDEX_DEFAULT = 5;
  public static final String MAX_ERROR_RECORDS_PER_STAGE = "production.maxErrorRecordsPerStage";
  public static final int MAX_ERROR_RECORDS_PER_STAGE_DEFAULT = 100;
  public static final String MAX_PIPELINE_ERRORS = "production.maxPipelineErrors";
  public static final int MAX_PIPELINE_ERRORS_DEFAULT = 100;

  public static final String STOP_PIPELINE_MESSAGE = "Requested via REST API";



}
