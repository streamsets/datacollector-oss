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
package com.streamsets.pipeline.runner;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.Utils;

import java.util.Iterator;
import java.util.List;

public class BatchImpl implements Batch {
  private final String instanceName;
  private final List<Record> records;
  private final String sourceOffset;
  private boolean got;

  public BatchImpl(String instanceName, SourceOffsetTracker offsetTracker, List<Record> records) {
    this.instanceName = instanceName;
    this.records = records;
    sourceOffset = offsetTracker.getOffset();
    got = false;
  }

  @Override
  public String getSourceOffset() {
    return sourceOffset;
  }

  @Override
  public Iterator<Record> getRecords() {
    Preconditions.checkState(!got, "The record iterator can be obtained only once");
    got = true;
    return records.iterator();
  }

  public int getSize() {
    return records.size();
  }

  @Override
  public String toString() {
    return Utils.format("BatchImpl[instance='{}' size='{}', iterated='{}']", instanceName, records.size(), got);
  }

}
