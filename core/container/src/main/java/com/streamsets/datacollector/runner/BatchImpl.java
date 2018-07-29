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
package com.streamsets.datacollector.runner;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BatchImpl implements Batch {
  private final String instanceName;
  private final List<Record> records;
  private final String sourceEntity;
  private final String sourceOffset;

  public BatchImpl(String instanceName, String sourceEntity, String sourceOffset, List<Record> records) {
    this.instanceName = instanceName;
    this.records = Collections.unmodifiableList(records);
    this.sourceEntity = sourceEntity;
    this.sourceOffset = sourceOffset;
  }

  @Override
  public String getSourceEntity() {
    return sourceEntity;
  }

  @Override
  public String getSourceOffset() {
    return sourceOffset;
  }

  @Override
  public Iterator<Record> getRecords() {
    return records.iterator();
  }

  public int getSize() {
    return records.size();
  }

  @Override
  public String toString() {
    return Utils.format("BatchImpl[instance='{}' size='{}']", instanceName, records.size());
  }

}
