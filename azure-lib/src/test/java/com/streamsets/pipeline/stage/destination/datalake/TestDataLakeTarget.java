/**
 * Copyright 2016 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.datalake;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestDataLakeTarget {
  private ADLStoreClient client = null;
  MockWebServer server = null;
  private static String accountFQDN;
  private static String dummyToken;

  @Before
  public void setup() throws IOException {
    server = new MockWebServer();
    QueueDispatcher dispatcher = new QueueDispatcher();
    dispatcher.setFailFast(new MockResponse().setResponseCode(400));
    server.setDispatcher(dispatcher);
    server.start();
    this.accountFQDN = server.getHostName() + ":" + server.getPort();
    this.dummyToken = "testDummyAdToken";
  }

  @After
  public void teardown() throws IOException {
    server.shutdown();
  }

  private DataLakeConfigBean getDataLakeTargetConfig() {
    DataLakeConfigBean conf = new DataLakeConfigBean();
    conf.accountFQDN = accountFQDN;
    conf.clientId = "1";
    conf.clientKey = dummyToken;
    conf.dirPathTemplate = "/tmp/out/";
    conf.fileNameTemplate = "test";
    conf.timeDriver = "${time:now()}";
    conf.dataFormat = DataFormat.TEXT;
    conf.dataFormatConfig.textFieldPath = "/text";
    conf.timeZoneID = "UTC";

    return conf;
  }

  private List<Record> createStringRecords() {
    List<Record> records = new ArrayList<>(9);
    final String TEST_STRING = "test";
    final String MIME = "text/plain";
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("text", "s:" + i, (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create((TEST_STRING+ i)));
      records.add(r);
    }
    return records;
  }

}
