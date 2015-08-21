/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ${groupId}.stage.destination.sample;

import ${groupId}.stage.lib.sample.Errors;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class SampleTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(SampleTarget.class);

  private final String sampleConfig;

  /**
   * Creates a new instance of this target. This target is an example and does not
   * actually write to any destination.
   *
   * @param sampleConfig is a dummy configuration parameter for this example.
   */
  public SampleTarget(String sampleConfig) {
    // Persist configuration values to member variables only.
    this.sampleConfig = sampleConfig;
  }


  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    if (sampleConfig.equals("invalidValue")) {
      issues.add(
          getContext().createConfigIssue(
              Groups.SAMPLE.name(), "sampleConfig", Errors.SAMPLE_00, "Here's what's wrong..."
          )
      );
    }

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      try {
        write(record);
      } catch (Exception e) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            Iterator<Record> failedRecords = batch.getRecords();
            while (failedRecords.hasNext()) {
              getContext().toError(record, Errors.SAMPLE_01, e.toString());
            }
            break;
          case STOP_PIPELINE:
            throw new StageException(Errors.SAMPLE_01, e.toString());
          default:
            throw new IllegalStateException(
                Utils.format("It should never happen. OnError '{}'", getContext().getOnErrorRecord(), e)
            );
        }
      }
    }
  }

  /**
   * Writes a single record to the destination.
   *
   * @param record the record to write to the destination.
   * @throws OnRecordErrorException when a record cannot be written.
   */
  private void write(Record record) throws OnRecordErrorException {
    // This is a contrived example, normally you may be performing an operation that could throw
    // an exception or produce an error condition. In that case you can throw an OnRecordErrorException
    // to send this record to the error pipeline with some details.
    if (!record.has("/someField")) {
      throw new OnRecordErrorException(Errors.SAMPLE_01, record, "exception detail message.");
    }

    // TODO: write the records to your final destination
  }
}
