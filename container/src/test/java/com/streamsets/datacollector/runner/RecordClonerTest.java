/*
 * Copyright 2020 StreamSets Inc.
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

import com.streamsets.datacollector.record.EventRecordImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Record;
import org.junit.Test;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class RecordClonerTest {

  private static final RecordCloner clone = new RecordCloner(false);
  private static final RecordCloner ref = new RecordCloner(true);

  @Test
  public void testRecordClone() {
    Record record = new RecordImpl("a", "b", null, null);
    assertSame(record, ref.cloneRecordIfNeeded(record));
    assertNotSame(record, clone.cloneRecordIfNeeded(record));
  }

  @Test
  public void testEventClone() {
    EventRecord record = new EventRecordImpl("type", 0, "a", "b", null, null);
    assertSame(record, ref.cloneEventIfNeeded(record));
    assertNotSame(record, clone.cloneEventIfNeeded(record));
  }
}
