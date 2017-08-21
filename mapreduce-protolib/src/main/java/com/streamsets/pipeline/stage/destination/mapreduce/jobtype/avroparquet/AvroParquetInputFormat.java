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
package com.streamsets.pipeline.stage.destination.mapreduce.jobtype.avroparquet;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 */
public class AvroParquetInputFormat extends InputFormat {

  public static class FileSplit extends InputSplit implements Writable {

    String input;
    String output;

    // Required by MR
    public FileSplit() {
    }

    public FileSplit(String input, String output) {
      this.input = input;
      this.output = output;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeUTF(input);
      dataOutput.writeUTF(output);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.input = dataInput.readUTF();
      this.output = dataInput.readUTF();
    }
  }

  public static class FileRecordReader extends RecordReader<String, String> {

    FileSplit split;
    boolean processed;

    public FileRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      this.split = (FileSplit) inputSplit;
      this.processed = false;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      boolean ret = !processed;
      processed = true;
      return ret;
    }

    @Override
    public String getCurrentKey() throws IOException, InterruptedException {
      return split.input;
    }

    @Override
    public String getCurrentValue() throws IOException, InterruptedException {
      return split.output;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }
  }


  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    return ImmutableList.<InputSplit>of(
      new FileSplit(jobContext.getConfiguration().get(AvroParquetConstants.INPUT_FILE), jobContext.getConfiguration().get(AvroParquetConstants.OUTPUT_DIR))
    );
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new FileRecordReader(inputSplit, taskAttemptContext);
  }
}
