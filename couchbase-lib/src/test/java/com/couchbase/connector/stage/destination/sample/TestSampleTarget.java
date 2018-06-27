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
package com.couchbase.connector.stage.destination.sample;

import org.junit.Test;

public class TestSampleTarget {
  @Test
  public void testWriteSingleRecord() throws Exception {
    /*
      TargetRunner runner = new TargetRunner.Builder(SampleDTarget.class)
        .addConfiguration("username", "value")
        .addConfiguration("password", "password")
        .addConfiguration("URL","URL")
        .addConfiguration("bucket", "bucket")
        .addConfiguration("documentKey", "test")
        .build();

    runner.runInit();

    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("first", Field.create("John"));
    fields.put("last", Field.create("Smith"));
    fields.put("someField", Field.create("some value"));
    fields.put("someField", Field.create("some value"));
    record.set(Field.create(fields));


    runner.runWrite(Arrays.asList(record));

    // Here check the data destination. E.g. a mock.

    runner.runDestroy(); 
      
     */
    return;
  }
}
