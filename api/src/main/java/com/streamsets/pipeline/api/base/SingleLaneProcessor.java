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
package com.streamsets.pipeline.api.base;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Record;

import java.util.List;

public abstract class SingleLaneProcessor extends BaseProcessor {

  public interface SingleLaneBatchMaker {
    public void addRecord(Record record);
  }

  private String outputLane;

  public SingleLaneProcessor() {
    setRequiresSuperInit();
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    if (getContext().getOutputLanes().size() != 1) {
      issues.add(getContext().createConfigIssue(null, null, Errors.API_00, getInfo().getInstanceName(),
                                                getContext().getOutputLanes().size()));
    } else {
      outputLane = getContext().getOutputLanes().iterator().next();
    }
    setSuperInitCalled();
    return issues;
  }

  @Override
  public void process(final Batch batch, final BatchMaker batchMaker) throws StageException {
    SingleLaneBatchMaker slBatchMaker = new SingleLaneBatchMaker() {
      @Override
      public void addRecord(Record record) {
        batchMaker.addRecord(record, outputLane);
      }
    };
    process(batch, slBatchMaker);
  }

  public abstract void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker)
      throws StageException;

}
