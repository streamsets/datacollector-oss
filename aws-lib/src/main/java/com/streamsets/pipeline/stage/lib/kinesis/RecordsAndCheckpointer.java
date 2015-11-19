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
package com.streamsets.pipeline.stage.lib.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;

/**
 * Wrapper for a batch of records and their checkpointer instance.
 */
public class RecordsAndCheckpointer {
  private final List<Record> records;
  private final IRecordProcessorCheckpointer checkpointer;

  public RecordsAndCheckpointer(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
    Preconditions.checkNotNull(checkpointer);
    this.records = records;
    this.checkpointer = checkpointer;
  }

  public RecordsAndCheckpointer(IRecordProcessorCheckpointer checkpointer) {
    this(Collections.<Record>emptyList(), checkpointer);
  }

  public List<Record> getRecords() {
    return records;
  }

  public IRecordProcessorCheckpointer getCheckpointer() {
    return checkpointer;
  }
}
