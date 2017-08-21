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
package com.streamsets.pipeline.stage.destination;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Test specific Input Format that primarily executes various actions based on configuration and never generates any
 * splits. Idea is to validate various states when submitting mapreduce job, but not to validate the mapreduce job
 * itself.
 */
public class SimpleTestInputFormat extends InputFormat {

  public static final String THROW_EXCEPTION = "sdc.exception";
  public static final String FILE_LOCATION = "sdc.file.path";
  public static final String FILE_VALUE = "sdc.file.value";

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();

    if(conf.getBoolean(THROW_EXCEPTION, false)) {
      throw new IOException("Throwing exception as instructed, failure in bootstraping MR job.");
    }

    String fileLocation = conf.get(FILE_LOCATION);
    if(fileLocation != null) {
      FileUtils.writeStringToFile(new File(fileLocation), conf.get(FILE_VALUE));
    }

    return Collections.emptyList();
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return null;
  }
}
