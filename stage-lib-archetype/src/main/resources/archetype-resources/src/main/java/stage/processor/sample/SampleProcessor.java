/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package ${groupId}.stage.processor.sample;

import ${groupId}.stage.lib.sample.Errors;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SampleProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(SampleProcessor.class);

  private final String sampleConfig;

  /**
   * Creates a new instance of this processor. This processor is an example and is
   * effectively a no-op.
   *
   * @param sampleConfig is a dummy configuration parameter for this example.
   */
  public SampleProcessor(String sampleConfig) {
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
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    // TODO: Implement your record processing here, then add to the output batch.

    // This example is a no-op
    batchMaker.addRecord(record);
  }

}