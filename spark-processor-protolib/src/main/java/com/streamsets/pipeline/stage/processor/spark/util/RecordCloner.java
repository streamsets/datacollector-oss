/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.spark.util;

import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;

public class RecordCloner {

  private RecordCloner() {
  }

  /**
   * Kryo loads the RecordImpl in Spark's classloader. So this one clones it to this stage's classloader.
   *
   * @param record Record to be cloned
   * @param context The context of the {@linkplain Processor} to use to clone the record
   * @return Cloned record
   */
  public static Record clone(Record record, Processor.Context context) {
    // Kryo loads the RecordImpl class during deserialization in a Spark's classloader.
    // So directly using the deserialized RecordImpl gives a ClassCastException (RecordImpl -> RecordImpl).
    // So create a new record and set its root field to be the deserialized one's root field.
    Record r = context.createRecord(record);
    r.set(record.get());
    r.getHeader().setAllAttributes(record.getHeader().getAllAttributes());
    return r;
  }
}