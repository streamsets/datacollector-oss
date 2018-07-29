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
package com.streamsets.pipeline.stage.destination.datalake;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.ADLStoreOptions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Currently tests do not include client auth because of lack of support in MockWebServer in https protocol
 * so we need to implement after auth requests in https/SSL if necessary
 */
@Ignore
public class DataLakeTargetIT {
  private static ADLStoreClient client = null;
  private static String accountFQDN;
  private static String authTokenEndpoint;
  private static String dummyToken;
  private static WebServerBuilder webServer;

  @BeforeClass
  public static void setup() throws IOException {
    webServer = new WebServerBuilder();

    authTokenEndpoint = webServer.getAuthTokenEndPoint();
    accountFQDN = webServer.getAccountFQDN();
    dummyToken = "testDummyAdToken";

    client = ADLStoreClient.createClient(accountFQDN, dummyToken);
    client.setOptions(new ADLStoreOptions().setInsecureTransport());
  }

  @AfterClass
  public static void teardown() throws IOException {
    webServer.shutdown();
  }

  @Test
  public void testEmptyRecord() throws Exception {
    webServer.enqueueFileInfoSuccessResponse()
        .enqueueTokenSuccess()
        .enqueueClientSuccessResponse()
        .enqueueFileInfoSuccessResponse()
        .enqueueFileInfoSuccessResponse();

    DataLakeTarget target = new DataLakeTargetBuilder()
        .accountFQDN(accountFQDN)
        .authTokenEndpoint(authTokenEndpoint)
        .build();

    TargetRunner targetRunner = new TargetRunner.Builder(DataLakeDTarget.class, mockTarget(target))
        .build();

    targetRunner.runInit();

    List<Record> records = new ArrayList<>();

    try {
      targetRunner.runWrite(records);
    } finally {
      targetRunner.runDestroy();
    }

    Assert.assertEquals(0, targetRunner.getErrors().size());
    Assert.assertEquals(0, targetRunner.getErrorRecords().size());
  }

  @Test
  public void testWrite() throws Exception {
    webServer.enqueueFileInfoSuccessResponse()
        .enqueueTokenSuccess()
        .enqueueClientSuccessResponse()
        .enqueueFileInfoSuccessResponse();

    DataLakeTarget target = new DataLakeTargetBuilder()
        .accountFQDN(accountFQDN)
        .authTokenEndpoint(authTokenEndpoint)
        .filesPrefix("sdc-")
        .build();

    TargetRunner targetRunner = new TargetRunner.Builder(DataLakeDTarget.class, mockTarget(target))
        .build();

    targetRunner.runInit();

    final int totalNumber = 1;
    List<Record> records = createStringRecords(totalNumber);

    try {
      targetRunner.runWrite(records);
    } finally {
      targetRunner.runDestroy();
    }

    Record record = records.get(0);
    record.getHeader().getSourceId();

    Assert.assertEquals(0, targetRunner.getErrors().size());
    Assert.assertEquals(0, targetRunner.getErrorRecords().size());
  }

  @Test
  public void testWriteWith401Error() throws Exception {
    webServer.enqueueFileInfoSuccessResponse()
        .enqueueTokenSuccess()
        .enqueueClientSuccessResponse()
        // access token expiration error with 5 retries (default)
        .enqueueTokenFailureResponse()
        .enqueueTokenFailureResponse()
        .enqueueTokenFailureResponse()
        .enqueueTokenFailureResponse()
        .enqueueTokenFailureResponse()
        // acquire renewed token
        .enqueueTokenSuccess()
        .enqueueClientSuccessResponse()
        // retry the request returns success
        .enqueueFileInfoSuccessResponse();

    DataLakeTarget target = new DataLakeTargetBuilder()
        .accountFQDN(accountFQDN)
        .authTokenEndpoint(authTokenEndpoint)
        .filesPrefix("sdc-")
        .build();

    TargetRunner targetRunner = new TargetRunner.Builder(DataLakeDTarget.class, mockTarget(target))
        .setOnRecordError(OnRecordError.DISCARD)
        .build();

    targetRunner.runInit();

    final int totalNumber = 1;
    List<Record> records = createStringRecords(totalNumber);

    try {
      targetRunner.runWrite(records);

      Assert.assertEquals(0, targetRunner.getErrors().size());
      Assert.assertEquals(0, targetRunner.getErrorRecords().size());
    } finally {
      targetRunner.runDestroy();
    }
  }

  @Ignore
  @Test
  public void testExceptionWhileWriting() throws Exception {
    // one success write record and then 5 retries to write record to fail
    webServer.enqueueFileInfoSuccessResponse()
        .enqueueTokenSuccess()
        .enqueueClientSuccessResponse()
        .enqueueFileInfoSuccessResponse()
        .enqueueFileInfoSuccessResponse();

    // default 5 retries
    final int retries = 5;
    for(int i = 0; i < retries; i++) {
      webServer.enqueueFileInfoFailureResponse()
          .enqueueFileInfoFailureResponse()
          .enqueueFileInfoFailureResponse()
          .enqueueFileInfoFailureResponse()
          .enqueueFileInfoFailureResponse();
    }

    DataLakeTarget target = new DataLakeTargetBuilder()
        .accountFQDN(accountFQDN)
        .authTokenEndpoint(authTokenEndpoint)
        .build();

    TargetRunner targetRunner = new TargetRunner.Builder(DataLakeDTarget.class, mockTarget(target))
        .setOnRecordError(OnRecordError.DISCARD)
        .build();

    targetRunner.runInit();

    final int totalNumber = 2;
    List<Record> records = createStringRecords(totalNumber);

    try {
      targetRunner.runWrite(records);
    } finally {
      targetRunner.runDestroy();
    }

    Assert.assertEquals(1, targetRunner.getErrorRecords().size());
    Assert.assertEquals(Errors.ADLS_03.getCode(), targetRunner.getErrorRecords().get(0).getHeader().getErrorCode());
  }

  @Test
  public void testUniquePrefix() throws Exception {
    DataLakeTarget target = new DataLakeTargetBuilder()
        .accountFQDN("localhost:9000")
        .authTokenEndpoint("localhost:9000")
        .filesPrefix("sdc-${sdc:id()}")
        .build();

  }

  private List<Record> createStringRecords(int size) {
    List<Record> records = new ArrayList<>(size);
    final String TEST_STRING = "test";
    final String MIME = "text/plain";
    for (int i = 0; i < size; i++) {
      Record r = RecordCreator.create("text", "s:" + i, (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create((TEST_STRING+ i)));
      records.add(r);
    }
    return records;
  }

  /**
   * Mock ADLS client which only uses HTTP protocol
   * @param target
   */
  private DataLakeTarget mockTarget(DataLakeTarget target) throws IOException {
    DataLakeTarget adlsTarget = Mockito.spy(target);
    Mockito.doReturn(client).when(adlsTarget).createClient(
        Mockito.any(String.class),
        Mockito.any(String.class),
        Mockito.any(String.class),
        Mockito.any(String.class)
    );
    return adlsTarget;
  }
}
