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
package com.streamsets.pipeline.lib.stage.source.kafka;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.record.RecordImpl;

public class TestKafkaProcuder {

  private static final String TEST_STRING = "TestFileErrorRecordStore";
  private static final String MIME = "application/octet-stream";

  public static void main(String[] args) {
    KafkaProducer p = new KafkaProducer("BB", "1", new KafkaBroker("localhost", 9001), PayloadType.STRING,
      PartitionStrategy.FIXED);
    p.init();

    Record r1 = new RecordImpl("s", "s:1", TEST_STRING.getBytes(), MIME);
    r1.set(Field.create("Hello World1"));
    ((RecordImpl)r1).getHeader().setTrackingId("t1");

    Record r2 = new RecordImpl("s", "s:1", TEST_STRING.getBytes(), MIME);
    r2.set(Field.create("Hello World2"));
    ((RecordImpl)r2).getHeader().setTrackingId("t2");

    Record r3 = new RecordImpl("s", "s:1", TEST_STRING.getBytes(), MIME);
    r3.set(Field.create("Hello World3"));
    ((RecordImpl)r3).getHeader().setTrackingId("t3");


    p.enqueueRecord(r1);
    p.enqueueRecord(r2);
    p.enqueueRecord(r3);
    p.write();
  }
}
