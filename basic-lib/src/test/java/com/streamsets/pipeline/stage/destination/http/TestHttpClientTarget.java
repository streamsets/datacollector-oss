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
package com.streamsets.pipeline.stage.destination.http;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.http.AbstractHttpStageTest;
import com.streamsets.pipeline.lib.http.HttpCompressionType;
import com.streamsets.pipeline.lib.http.HttpConstants;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.iq80.snappy.SnappyFramedInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class TestHttpClientTarget extends AbstractHttpStageTest {
  private static final String CONTENT_TYPE = "Content-type";
  private Server server;
  private static boolean serverRequested = false;
  private static String requestPayload = null;
  private static String requestContentType = null;
  private static boolean returnErrorResponse = false;
  private static String compressionType = null;

  @Before
  public void setUp() throws Exception {
    int port =  getFreePort();
    server = new Server(port);
    server.setHandler(new AbstractHandler() {
      @Override
      public void handle(
          String target,
          Request baseRequest,
          HttpServletRequest request,
          HttpServletResponse response
      ) throws IOException, ServletException {
        serverRequested = true;
        if (returnErrorResponse) {
          response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          return;
        }

        compressionType = request.getHeader(HttpConstants.CONTENT_ENCODING_HEADER);
        InputStream is = request.getInputStream();
        if (compressionType != null && compressionType.equals(HttpConstants.SNAPPY_COMPRESSION)) {
          is = new SnappyFramedInputStream(is, true);
        } else if (compressionType != null && compressionType.equals(HttpConstants.GZIP_COMPRESSION)) {
          is = new GZIPInputStream(is);
        }
        requestPayload = IOUtils.toString(is);
        requestContentType = request.getContentType();
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
      }
    });
    server.start();
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  public HttpClientTargetConfig getConf(String url) {
    HttpClientTargetConfig conf = new HttpClientTargetConfig();
    conf.httpMethod = HttpMethod.POST;
    conf.dataFormat = DataFormat.TEXT;
    conf.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    conf.dataGeneratorFormatConfig.textFieldPath = "/";
    conf.resourceUrl = url;
    return conf;
  }

  @Override
  public List<Stage.ConfigIssue> runStageValidation(String url) throws Exception {
    HttpClientTargetConfig config = getConf("http://localhost:10000");
    config.client.useProxy = true;
    config.client.proxy.uri = url;

    HttpClientTarget target = PowerMockito.spy(new HttpClientTarget(config));

    TargetRunner runner = new TargetRunner.Builder(HttpClientDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    return runner.runValidateConfigs();
  }

  public static int getFreePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }

  @Test
  public void testSingleRequestPerBatch() throws Exception {
    HttpClientTargetConfig config = getConf(server.getURI().toString());
    config.singleRequestPerBatch = true;
    serverRequested = false;
    requestPayload = null;
    requestContentType = null;
    returnErrorResponse = false;
    testHttpTarget(config);
    Assert.assertTrue(serverRequested);
    Assert.assertNotNull(requestPayload);
    Assert.assertTrue(requestPayload.contains("a\nb\n"));
    Assert.assertNotNull(requestContentType);
    Assert.assertEquals(MediaType.TEXT_PLAIN, requestContentType);
  }

  @Test
  public void testSingleRequestPerRecord() throws Exception {
    HttpClientTargetConfig config = getConf(server.getURI().toString());
    config.singleRequestPerBatch = false;
    serverRequested = false;
    requestPayload = null;
    requestContentType = null;
    returnErrorResponse = false;
    testHttpTarget(config);
    Assert.assertTrue(serverRequested);
    Assert.assertNotNull(requestPayload);
    Assert.assertTrue(requestPayload.contains("a") || requestPayload.contains("b"));
    Assert.assertNotNull(requestContentType);
    Assert.assertNotNull(requestContentType);
    Assert.assertEquals(MediaType.TEXT_PLAIN, requestContentType);
  }

  @Test
  public void testSingleRequestPerBatchFormSubmit() throws Exception {
    HttpClientTargetConfig config = getConf(server.getURI().toString());
    config.singleRequestPerBatch = true;
    config.headers.put(CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED);
    serverRequested = false;
    requestPayload = null;
    requestContentType = null;
    returnErrorResponse = false;
    testHttpTarget(config);
    Assert.assertTrue(serverRequested);
    Assert.assertNotNull(requestPayload);
    Assert.assertTrue(requestPayload.contains("a\nb\n"));
    Assert.assertNotNull(requestContentType);
    Assert.assertEquals(MediaType.APPLICATION_FORM_URLENCODED, requestContentType);
  }

  @Test
  public void testSingleRequestPerRecordFormSubmit() throws Exception {
    HttpClientTargetConfig config = getConf(server.getURI().toString());
    config.singleRequestPerBatch = false;
    config.headers.put(CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED);
    serverRequested = false;
    requestPayload = null;
    requestContentType = null;
    returnErrorResponse = false;
    testHttpTarget(config);
    Assert.assertTrue(serverRequested);
    Assert.assertNotNull(requestPayload);
    Assert.assertTrue(requestPayload.contains("a") || requestPayload.contains("b"));
    Assert.assertNotNull(requestContentType);
    Assert.assertNotNull(requestContentType);
    Assert.assertEquals(MediaType.APPLICATION_FORM_URLENCODED, requestContentType);
  }

  @Test
  public void testSnappyHttpCompression() throws Exception {
    HttpClientTargetConfig config = getConf(server.getURI().toString());
    config.singleRequestPerBatch = true;
    config.client.httpCompression = HttpCompressionType.SNAPPY;
    serverRequested = false;
    requestPayload = null;
    requestContentType = null;
    returnErrorResponse = false;
    testHttpTarget(config);
    Assert.assertTrue(serverRequested);
    Assert.assertNotNull(requestPayload);
    Assert.assertTrue(requestPayload.contains("a\nb\n"));
    Assert.assertEquals(compressionType, HttpConstants.SNAPPY_COMPRESSION);
    Assert.assertNotNull(requestContentType);
    Assert.assertEquals(MediaType.TEXT_PLAIN, requestContentType);
  }


  @Test
  public void testGZipHttpCompression() throws Exception {
    HttpClientTargetConfig config = getConf(server.getURI().toString());
    config.singleRequestPerBatch = true;
    config.client.httpCompression = HttpCompressionType.GZIP;
    serverRequested = false;
    requestPayload = null;
    requestContentType = null;
    returnErrorResponse = false;
    testHttpTarget(config);
    Assert.assertTrue(serverRequested);
    Assert.assertNotNull(requestPayload);
    Assert.assertTrue(requestPayload.contains("a\nb\n"));
    Assert.assertEquals(compressionType, HttpConstants.GZIP_COMPRESSION);
    Assert.assertNotNull(requestContentType);
    Assert.assertEquals(MediaType.TEXT_PLAIN, requestContentType);
  }

  private void testHttpTarget(HttpClientTargetConfig config) throws Exception {
    HttpClientTarget target = new HttpClientTarget(config);
    TargetRunner runner = new TargetRunner.Builder(HttpClientDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Record> input = new ArrayList<>();
    input.add(createRecord("a"));
    input.add(createRecord("b"));
    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
    runner.runInit();
    try {
      runner.runWrite(input);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());

      List<Record> responseRecords = runner.getSourceResponseSink().getResponseRecords();
      Assert.assertEquals(0, responseRecords.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testHttpTargetSendingResponseBackToOrigin() throws Exception {
    HttpClientTargetConfig config = getConf(server.getURI().toString());
    config.responseConf.sendResponseToOrigin = true;

    HttpClientTarget target = new HttpClientTarget(config);
    TargetRunner runner = new TargetRunner.Builder(HttpClientDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Record> input = new ArrayList<>();
    input.add(createRecord("a"));
    input.add(createRecord("b"));
    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
    runner.runInit();
    try {
      runner.runWrite(input);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());

      List<Record> responseRecords = runner.getSourceResponseSink().getResponseRecords();
      Assert.assertEquals(2, responseRecords.size());

    } finally {
      runner.runDestroy();
    }
  }

  public static Record createRecord(String str) {
    Record record = RecordCreator.create();
    record.set(Field.create(str));
    return record;
  }


  @Test
  public void testSingleRequestPerBatchInvalidURL() throws Exception {
    HttpClientTargetConfig config = getConf("http://localho:2344");
    config.singleRequestPerBatch = true;
    testErrorHandling(config);
  }

  @Test
  public void testSingleRequestPerRecordInvalidURL() throws Exception {
    HttpClientTargetConfig config = getConf("http://localho:2344");
    config.singleRequestPerBatch = false;
    returnErrorResponse = true;
    testErrorHandling(config);
  }

  @Test
  public void testSingleRequestPerBatchServerError() throws Exception {
    HttpClientTargetConfig config = getConf(server.getURI().toString());
    config.singleRequestPerBatch = true;
    returnErrorResponse = true;
    testErrorHandling(config);
  }

  @Test
  public void testSingleRequestPerRecordServerError() throws Exception {
    HttpClientTargetConfig config = getConf(server.getURI().toString());
    config.singleRequestPerBatch = false;
    returnErrorResponse = true;
    testErrorHandling(config);
  }


  private void testErrorHandling(HttpClientTargetConfig config) throws Exception {
    HttpClientTarget target = new HttpClientTarget(config);
    TargetRunner runner = new TargetRunner.Builder(HttpClientDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    List<Record> input = new ArrayList<>();
    input.add(createRecord("a"));
    input.add(createRecord("b"));
    Assert.assertTrue(runner.runValidateConfigs().isEmpty());
    runner.runInit();
    try {
      runner.runWrite(input);
      Assert.assertFalse(runner.getErrorRecords().isEmpty());
      List<Record> records = runner.getErrorRecords();
      Assert.assertEquals(records.size(), 2);
    } finally {
      runner.runDestroy();
    }
  }

  public HttpClientTargetConfig getJsonArrayConf(String url) {
    HttpClientTargetConfig conf = new HttpClientTargetConfig();
    conf.httpMethod = HttpMethod.POST;
    conf.dataFormat = DataFormat.JSON;
    conf.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    conf.dataGeneratorFormatConfig.jsonMode = JsonMode.ARRAY_OBJECTS;
    conf.resourceUrl = url;
    return conf;
  }

  @Test
  public void testJSONArraryOfObjectsReqPerBatch() throws Exception {
    HttpClientTargetConfig config = getJsonArrayConf(server.getURI().toString());
    config.singleRequestPerBatch = true;
    serverRequested = false;
    requestPayload = null;
    requestContentType = null;
    returnErrorResponse = false;
    testHttpTarget(config);
    Assert.assertTrue(serverRequested);
    Assert.assertNotNull(requestPayload);
    Assert.assertTrue(requestPayload.contains("[\"a\",\"b\"]"));
    Assert.assertNotNull(requestContentType);
    Assert.assertEquals(MediaType.APPLICATION_JSON, requestContentType);
  }

  @Test
  public void testJSONArraryOfObjectsReqPerRecord() throws Exception {
    HttpClientTargetConfig config = getJsonArrayConf(server.getURI().toString());
    config.singleRequestPerBatch = false;
    serverRequested = false;
    requestPayload = null;
    requestContentType = null;
    returnErrorResponse = false;
    testHttpTarget(config);
    Assert.assertTrue(serverRequested);
    Assert.assertNotNull(requestPayload);
    Assert.assertTrue(requestPayload.contains("[\"a\"]") || requestPayload.contains("[\"b\"]"));
    Assert.assertNotNull(requestContentType);
    Assert.assertEquals(MediaType.APPLICATION_JSON, requestContentType);
  }
}
