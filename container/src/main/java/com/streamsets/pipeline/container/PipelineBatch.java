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
package com.streamsets.pipeline.container;

import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.List;

public class PipelineBatch {
  private String previousBatchId;
  private boolean preview;
  private List<Record> records;
  private String batchId;

  public PipelineBatch(String previousBatchId) {
    this(previousBatchId, false);
  }

  public PipelineBatch(String previousBatchId, boolean preview) {
    this.previousBatchId = previousBatchId;
    this.preview = preview;
    records = new ArrayList<Record>();
  }

  public boolean isPreview() {
    return preview;
  }

  public String getPreviousBatchId() {
    return previousBatchId;
  }

  public boolean isEmpty() {
    return records.isEmpty();
  }

  public void setBatchId(String batchId) {
    this.batchId = batchId;
  }

  public String getBatchId() {
    return batchId;
  }

  public List<Record> getRecords() {
    return records;
  }

  public List<Record> drain() {
    List<Record> drain = records;
    records = new ArrayList<Record>();
    return drain;
  }

  public void populate(List<Record> records) {
    this.records.addAll(records);
  }

  public void pipeCheckPoint(Pipe pipe) {
  }

}
