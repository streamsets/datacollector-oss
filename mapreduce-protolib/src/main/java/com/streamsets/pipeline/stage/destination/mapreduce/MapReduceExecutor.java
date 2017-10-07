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
package com.streamsets.pipeline.stage.destination.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.mapreduce.config.JobConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.config.MapReduceConfig;
import com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet.AvroParquetConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class MapReduceExecutor extends BaseExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceExecutor.class);

  private final MapReduceConfig mapReduceConfig;
  private final JobConfig jobConfig;
  private ErrorRecordHandler errorRecordHandler;

  @VisibleForTesting
  public boolean waitForCompletition;

  public MapReduceExecutor(MapReduceConfig mapReduceConfig, JobConfig jobConfig) {
    this.mapReduceConfig = mapReduceConfig;
    this.jobConfig = jobConfig;
    this.waitForCompletition = false;
  }

  @Override
  public List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    issues.addAll(mapReduceConfig.init(getContext(), "mapReduceConfig"));
    issues.addAll(jobConfig.init(getContext(), "jobConfig"));

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    return issues;
  }

  /**
   * Handy class to keep track of various ELs with the shared variables object.
   */
  private static class EvalContext {
    private ELVars variables;
    private Map<String, ELEval> evals;
    private Stage.Context context;

    public EvalContext(Stage.Context context) {
      this.context = context;
      this.variables = context.createELVars();
      this.evals = new HashMap<>();
    }

    public void setRecord(Record record) {
      RecordEL.setRecordInContext(variables, record);
    }

    public String evaluateToString(String name, String expr) throws ELEvalException {
      return evaluate(name, expr, String.class);
    }

    public <T> T evaluate(String name, String expr, Class<T> klass) throws ELEvalException {
      return getEval(name).eval(variables, expr, klass);
    }

    public ELEval getEval(String name) {
      if(evals.containsKey(name)) {
        return evals.get(name);
      }

      ELEval eval = context.createELEval(name);
      evals.put(name, eval);
      return eval;
    }
  }

  @Override
  public void write(Batch batch) throws StageException {
    EvalContext eval = new EvalContext(getContext());

    Iterator<Record> it = batch.getRecords();
    while(it.hasNext()) {
      final Record record = it.next();
      eval.setRecord(record);

      // Job configuration object is a clone of the original one that we're keeping in mapReduceConfig class
      final Configuration jobConfiguration = new Configuration(mapReduceConfig.getConfiguration());

      // Evaluate all dynamic properties and store them in the configuration job
      for(Map.Entry<String, String> entry : jobConfig.jobConfigs.entrySet()) {
        String key = eval.evaluateToString("jobConfigs", entry.getKey());
        String value = eval.evaluateToString("jobConfigs", entry.getValue());

        jobConfiguration.set(key, value);
      }

      // For build-in job creators, evaluate their properties and persist them in the MR config
      switch(jobConfig.jobType) {
        case AVRO_PARQUET:
          jobConfiguration.set(AvroParquetConstants.INPUT_FILE, eval.evaluateToString("inputFile", jobConfig.avroParquetConfig.inputFile));
          jobConfiguration.set(AvroParquetConstants.OUTPUT_DIR, eval.evaluateToString("outputDirectory", jobConfig.avroParquetConfig.outputDirectory));
          jobConfiguration.setBoolean(AvroParquetConstants.KEEP_INPUT_FILE, jobConfig.avroParquetConfig.keepInputFile);
          jobConfiguration.set(AvroParquetConstants.COMPRESSION_CODEC_NAME, eval.evaluateToString("compressionCodec", jobConfig.avroParquetConfig.compressionCodec));
          jobConfiguration.setInt(AvroParquetConstants.ROW_GROUP_SIZE, jobConfig.avroParquetConfig.rowGroupSize);
          jobConfiguration.setInt(AvroParquetConstants.PAGE_SIZE, jobConfig.avroParquetConfig.pageSize);
          jobConfiguration.setInt(AvroParquetConstants.DICTIONARY_PAGE_SIZE, jobConfig.avroParquetConfig.dictionaryPageSize);
          jobConfiguration.setInt(AvroParquetConstants.MAX_PADDING_SIZE, jobConfig.avroParquetConfig.maxPaddingSize);
          jobConfiguration.setBoolean(AvroParquetConstants.OVERWRITE_TMP_FILE, jobConfig.avroParquetConfig.overwriteTmpFile);
          break;
        case CUSTOM:
          // Nothing because custom is generic one that have no special config properties
          break;
        default:
          throw new UnsupportedOperationException("Unsupported JobType: " + jobConfig.jobType);
      }

      Job job = null;
      try {
        job = createAndSubmitJob(jobConfiguration);
      } catch (IOException|InterruptedException e) {
        LOG.error("Can't submit mapreduce job", e);
        errorRecordHandler.onError(new OnRecordErrorException(record, MapReduceErrors.MAPREDUCE_0005, e.getMessage()));
      }

      if(job != null) {
        MapReduceExecutorEvents.JOB_CREATED.create(getContext())
          .with("tracking-url", job.getTrackingURL())
          .with("job-id", job.getJobID().toString())
          .createAndSend();
      }
    }
  }

  private Job createAndSubmitJob(final Configuration configuration) throws IOException, InterruptedException {
    return mapReduceConfig.getUGI().doAs((PrivilegedExceptionAction<Job>) () -> {
      // Create new mapreduce job object
      Callable<Job> jobCreator = ReflectionUtils.newInstance(jobConfig.getJobCreator(), configuration);
      Job job = jobCreator.call();

      // In trace mode, dump all the configuration that we're using for the job
      if(LOG.isTraceEnabled()) {
        LOG.trace("Using the following configuration object for mapreduce job.");
        for(Map.Entry<String, String> entry : configuration) {
          LOG.trace("  Config: {}={}", entry.getKey(), entry.getValue());
        }
      }

      // Submit it for processing. Blocking mode is only for testing.
      job.submit();
      if(waitForCompletition) {
        job.waitForCompletion(true);
      }

      return job;
    });
  }
}
