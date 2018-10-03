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
package com.streamsets.pipeline.stage.processor.statsaggregation;

public class MetricAggregationConstants {

  public static final String METADATA_DPM_PIPELINE_ID = "dpm.pipeline.id";
  public static final String METADATA_DPM_PIPELINE_VERSION = "dpm.pipeline.version";
  public static final String ROOT_FIELD = "/";
  public static final String USER_PREFIX = "user.";
  public static final String PIPELINE_BATCH_PROCESSING = "pipeline.batchProcessing";
  public static final String PIPELINE_BATCH_COUNT = "pipeline.batchCount";
  public static final String PIPELINE_INPUT_RECORDS_PER_BATCH = "pipeline.inputRecordsPerBatch";
  public static final String PIPELINE_OUTPUT_RECORDS_PER_BATCH = "pipeline.outputRecordsPerBatch";
  public static final String PIPELINE_ERROR_RECORDS_PER_BATCH = "pipeline.errorRecordsPerBatch";
  public static final String PIPELINE_ERRORS_PER_BATCH = "pipeline.errorsPerBatch";
  public static final String PIPELINE_BATCH_INPUT_RECORDS = "pipeline.batchInputRecords";
  public static final String PIPELINE_BATCH_OUTPUT_RECORDS = "pipeline.batchOutputRecords";
  public static final String PIPELINE_BATCH_ERROR_RECORDS = "pipeline.batchErrorRecords";
  public static final String PIPELINE_BATCH_ERROR_MESSAGES = "pipeline.batchErrorMessages";
  public static final String STAGE_PREFIX = "stage.";
  public static final String BATCH_PROCESSING = ".batchProcessing";
  public static final String INPUT_RECORDS = ".inputRecords";
  public static final String OUTPUT_RECORDS = ".outputRecords";
  public static final String ERROR_RECORDS = ".errorRecords";
  public static final String STAGE_ERRORS = ".stageErrors";
  public static final String AGGREGATOR = "Aggregator";

}
