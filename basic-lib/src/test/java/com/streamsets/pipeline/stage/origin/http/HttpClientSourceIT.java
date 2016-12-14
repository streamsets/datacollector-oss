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
package com.streamsets.pipeline.stage.origin.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.http.AuthenticationType;
import com.streamsets.pipeline.lib.http.HttpMethod;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.util.http.HttpStageTestUtil;
import com.streamsets.pipeline.stage.util.http.HttpStageUtil;
import com.streamsets.testing.SingleForkNoReuseTest;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Currently tests do not include basic auth because of lack of support in JerseyTest
 * so we trust that the Jersey client we use implements auth correctly.
 */
@Category(SingleForkNoReuseTest.class)
public class HttpClientSourceIT extends JerseyTest {
  static long DELAY = 1100;

  private static final String NAMES_JSON = "{\"name\": \"adam\"}\r\n" +
          "{\"name\": \"joe\"}\r\n" +
          "{\"name\": \"sally\"}";
  private static final String EMTPY_RESPONSE = "\n";
  public static final String[] EXPECTED_NAMES = {"adam", "joe", "sally"};

  private static final int STATUS_TEST_FAIL = 487, STATUS_SLOW_DOWN = 420;
  private static long BASELINE_BACKOFF_MS = 100;
  private static final long SLOW_STREAM_UNIT_TIME = BASELINE_BACKOFF_MS;
  private static final int MAX_NUM_REQUEST_RETRIES = 100;

  private static boolean generalStreamResponseSent = false;

  private static Response entityOnlyOnFirstRequest(Object entity) {
    return onlyOnFirstRequest(Response.ok(entity));
  }

  private static Response onlyOnFirstRequest(Response.ResponseBuilder responseBuilder) {
    if (!generalStreamResponseSent) {
      generalStreamResponseSent = true;
      return responseBuilder.build();
    } else {
      return Response.ok().build();
    }
  }

  @Path("/stream")
  @Produces("application/json")
  public static class StreamResource {

    @GET
    public Response getStream() {
      return entityOnlyOnFirstRequest(NAMES_JSON);
    }

    private static final int NUM_SLOW_DOWN_RESPONSES = 3;
    private static int linearReqNum = 0;
    private static int expReqNum = 0;
    private static long lastReqLinear = 0;
    private static long lastReqExp = 0;

    private static int slowStreamReqNum = 0;

    private static final Response.ResponseBuilder SLOW_STREAM_KEEPALIVE_RESPONSE = Response.ok("\n");

    @GET
    @Path("/linear-backoff-ok")
    public Response getNamesWithLinearBackoff() {
      final long acceptableTime = BASELINE_BACKOFF_MS*linearReqNum;
      final Response.ResponseBuilder resp = buildBackoffResponseHelper(linearReqNum++, lastReqLinear, acceptableTime);
      lastReqLinear = System.currentTimeMillis();
      return resp.build();
    }

    @GET
    @Path("/exp-backoff-ok")
    public Response getNamesWithExponentialBackoff() {
      long acceptableTime = BASELINE_BACKOFF_MS;
      for (int i=1; i<expReqNum; i++) {
        acceptableTime*=2;
      }
      final Response.ResponseBuilder resp = buildBackoffResponseHelper(expReqNum++, lastReqExp, acceptableTime);
      lastReqExp = System.currentTimeMillis();
      return resp.build();
    }

    public Response.ResponseBuilder buildBackoffResponseHelper(int requestNum, long lastRequestTime, long acceptableTime) {
      final long timeSinceLastReq = System.currentTimeMillis() - lastRequestTime;
      if (timeSinceLastReq <= acceptableTime) {
        return Response.status(STATUS_TEST_FAIL);
      } else if (requestNum <= NUM_SLOW_DOWN_RESPONSES) {
        return Response.status(STATUS_SLOW_DOWN);
      } else {
        return Response.ok(NAMES_JSON);
      }
    }


    @GET
    @Path("/slow-stream")
    public Response getNamesWithSlowStream() {
      /*
          simulate the behavior described by the Twitter streaming API
          https://dev.twitter.com/streaming/overview/connecting

          1 unit = 100ms
          server newline every 1 unit
          client times out and reconnects after 3 units

          the script will be
          1. newline (empty batch)
          2. newline (empty batch)
          3. newline (empty batch)
          4. no response
          5. no response
          6. no response (timeout: empty batch)
          7. newline (empty batch)
          8. data (names batch)


       */

      Response.ResponseBuilder resp = SLOW_STREAM_KEEPALIVE_RESPONSE;
      switch (++slowStreamReqNum) {
        case 1:
          ThreadUtil.sleep(SLOW_STREAM_UNIT_TIME);
          break;
        case 2:
          ThreadUtil.sleep(SLOW_STREAM_UNIT_TIME);
          break;
        case 3:
          ThreadUtil.sleep(SLOW_STREAM_UNIT_TIME);
          break;
        case 4:
          // make the client time out on this one
          ThreadUtil.sleep(SLOW_STREAM_UNIT_TIME * 6);
          break;
        case 5:
          ThreadUtil.sleep(SLOW_STREAM_UNIT_TIME);
          break;
        case 6:
          resp = Response.ok(NAMES_JSON);
          break;
        default:
          resp = Response.status(STATUS_TEST_FAIL);
          break;
      }
      return resp.build();
    }

    @POST
    public Response postStream(String name) {
      Map<String, String> map = ImmutableMap.of("adam", "adam", "joe", "joe", "sally", "sally");
      String queriedName = map.get(name);
      final String entity = "{\"name\": \"" + queriedName + "\"}\r\n";
      return entityOnlyOnFirstRequest(entity);
    }
  }

  @Path("/nlstream")
  @Produces("application/json")
  public static class NewlineStreamResource {
    @GET
    public Response getStream() {
      return entityOnlyOnFirstRequest(
          "{\"name\": \"adam\"}\n" +
          "{\"name\": \"joe\"}\n" +
          "{\"name\": \"sally\"}");
    }
  }

  @Path("/xmlstream")
  @Produces("application/xml")
  public static class XmlStreamResource {
    @GET
    public Response getStream() {
      return entityOnlyOnFirstRequest(
          "<root>" +
          "<record>" +
          "<name>adam</name>" +
          "</record>" +
          "<record>" +
          "<name>joe</name>" +
          "</record>" +
          "<record>" +
          "<name>sally</name>" +
          "</record>" +
          "</root>"
      );
    }
  }

  @Path("/textstream")
  @Produces("application/text")
  public static class TextStreamResource {
    @GET
    public Response getStream() {
      return entityOnlyOnFirstRequest(
          "adam\r\n" +
          "joe\r\n" +
          "sally"
      );
    }
  }
  @Path("/slowstream")
  @Produces("application/text")
  public static class SlowTextStreamResource {
    @GET
    public Response getStream() throws InterruptedException {
      Thread.sleep(DELAY);
      return entityOnlyOnFirstRequest(
          "adam\r\n" +
              "joe\r\n" +
              "sally"
      );
    }
  }


  @Path("/headers")
  public static class HeaderRequired {
    @GET
    public Response getWithHeader(@Context HttpHeaders h) {
      // This endpoint will fail if a magic header isnt included
      String headerValue = h.getRequestHeaders().getFirst("abcdef");
      assertNotNull(headerValue);
      return onlyOnFirstRequest(Response.ok(
              NAMES_JSON
      ).header("X-Test-Header", "StreamSets").header("X-List-Header", ImmutableList.of("a", "b")));
    }
  }

  @Path("/preemptive")
  public static class PreemptiveAuthResource {

    @GET
    public Response get(@Context HttpHeaders h) {
      // This endpoint will fail if universal is used and expects preemptive auth (basic)
      String value = h.getRequestHeaders().getFirst("Authorization");
      assertNotNull(value);
      return entityOnlyOnFirstRequest(NAMES_JSON);
    }
  }

  @Path("/auth")
  @Singleton
  public static class AuthResource {

    int requestCount = 0;

    @GET
    public Response get(@Context HttpHeaders h) {
      // This endpoint supports the "universal" option which tells the client which auth to use on the 2nd request.
      requestCount++;
      String value = h.getRequestHeaders().getFirst("Authorization");
      if (value == null) {
        assertEquals(1, requestCount);
        throw new WebApplicationException(
            Response.status(401)
            .header("WWW-Authenticate", "Basic realm=\"WallyWorld\"")
            .build()
        );
      } else {
        assertTrue(requestCount > 1);
      }

      return entityOnlyOnFirstRequest(NAMES_JSON);
    }
  }

  @Path("/unauthorized")
  @Singleton
  public static class AlwaysUnauthorized {
    @GET
    public Response get() {
      return onlyOnFirstRequest(Response
          .status(401)
          .header("WWW-Authenticate", "Basic realm=\"WallyWorld\"")
      );
    }
  }

  @Override
  protected Application configure() {
    forceSet(TestProperties.CONTAINER_PORT, "0");
    return new ResourceConfig(
        Sets.newHashSet(
            StreamResource.class,
            NewlineStreamResource.class,
            TextStreamResource.class,
            SlowTextStreamResource.class,
            XmlStreamResource.class,
            PreemptiveAuthResource.class,
            AuthResource.class,
            HeaderRequired.class,
            AlwaysUnauthorized.class,
            HttpStageTestUtil.TestPostCustomType.class
        )
    );
  }

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return new GrizzlyWebTestContainerFactory();
  }

  @Override
  protected DeploymentContext configureDeployment() {
    return ServletDeploymentContext.forServlet(
        new ServletContainer(
            new ResourceConfig(
                Sets.newHashSet(
                    StreamResource.class,
                    NewlineStreamResource.class,
                    TextStreamResource.class,
                    SlowTextStreamResource.class,
                    XmlStreamResource.class,
                    PreemptiveAuthResource.class,
                    AuthResource.class,
                    HeaderRequired.class,
                    AlwaysUnauthorized.class,
                    HttpStageTestUtil.TestPostCustomType.class
                )
            )
        )
    ).build();
  }

  @Before
  public void resetServerStatus() {
    generalStreamResponseSent = false;
    StreamResource.slowStreamReqNum = 0;
    StreamResource.linearReqNum = 0;
    StreamResource.expReqNum = 0;
    StreamResource.lastReqLinear = 0;
    StreamResource.lastReqExp = 0;
  }

  @Test
  public void testStreamingHttp() throws Exception {
    DataFormat dataFormat = DataFormat.JSON;
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "stream";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = dataFormat;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    runBatchAndAssertNames(dataFormat, conf);
  }

  private void runBatchAndAssertNames(DataFormat dataFormat, HttpClientConfigBean conf) throws StageException {
    runBatchAndAssertNames(dataFormat, conf, EXPECTED_NAMES, false);
  }

  private void runBatchAndAssertNames(DataFormat dataFormat, HttpClientConfigBean conf, boolean delayStream) throws StageException {
    runBatchAndAssertNames(dataFormat, conf, EXPECTED_NAMES, delayStream);
  }

  private void runBatchAndAssertNames(DataFormat dataFormat, HttpClientConfigBean conf, String[] expectedNames, boolean delayStream) throws StageException {
    runBatchesAndAssertNames(dataFormat, conf, new String[][] {expectedNames}, delayStream);
  }

  private void runBatchesAndAssertNames(DataFormat dataFormat, HttpClientConfigBean conf, String[][] expectedNameBatches,
      boolean delayStream) throws StageException {
    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      for (String[] expectedNames : expectedNameBatches) {
        StageRunner.Output output = runner.runProduce(null, 1000);
        Map<String, List<Record>> recordMap = output.getRecords();
        List<Record> parsedRecords = new ArrayList<>(recordMap.get("lane"));
        // Before SDC-4337, this would return nothing
        if (delayStream) {
          // Before SDC-4337, this would return records 2 and 3, record 1 would be lost
          parsedRecords.addAll(getRecords(runner));
        }


        assertEquals(expectedNames.length, parsedRecords.size());

        for (int i = 0; i < parsedRecords.size(); i++) {
          if (dataFormat == DataFormat.JSON || dataFormat == DataFormat.XML) {
            assertTrue(parsedRecords.get(i).has("/name"));
          }
          assertEquals(expectedNames[i], extractValueFromRecord(parsedRecords.get(i), dataFormat));
        }
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStreamingPost() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "stream";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.POST;
    conf.requestBody = "adam";
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      List<Record> parsedRecords = getRecords(runner);

      assertEquals(1, parsedRecords.size());

      String[] names = { "adam" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertTrue(parsedRecords.get(i).has("/name"));
        assertEquals(names[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.JSON));
      }
    } finally {
      runner.runDestroy();
    }

  }

  @Test
  public void testDifferentContentTypesPost() throws Exception {
    final Random random = new Random();

    String fallbackContentType = "application/default";

    for (Map.Entry<String, String> requestEntry : HttpStageTestUtil.CONTENT_TYPE_TO_BODY.entrySet()) {

      String expectedContentType = requestEntry.getKey();

      HttpClientConfigBean conf = new HttpClientConfigBean();
      conf.client.authType = AuthenticationType.NONE;
      conf.httpMode = HttpClientMode.BATCH;
      conf.resourceUrl = getBaseUri() + "test/postCustomType";
      conf.client.readTimeoutMillis = 1000;
      conf.basic.maxBatchSize = 1;
      conf.basic.maxWaitTime = 1000;
      conf.pollingInterval = 1000;
      conf.httpMethod = HttpMethod.POST;
      conf.requestBody = requestEntry.getValue();
      conf.defaultRequestContentType = fallbackContentType;
      conf.dataFormat = DataFormat.JSON;
      conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

      if (StringUtils.isBlank(expectedContentType)) {
        expectedContentType = fallbackContentType;
      } else {
        String contentTypeHeader = HttpStageUtil.CONTENT_TYPE_HEADER;
        String header = HttpStageTestUtil.randomizeCapitalization(random, contentTypeHeader);
        conf.headers.put(header.toString(), expectedContentType);
      }

      HttpClientSource origin = new HttpClientSource(conf);

      SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
          .addOutputLane("lane")
          .build();
      runner.runInit();

      try {
        List<Record> parsedRecords = getRecords(runner);

        assertEquals(1, parsedRecords.size());
        final Record record = parsedRecords.get(0);
        assertTrue(record.has("/Content-Type"));
        assertEquals(expectedContentType, record.get("/Content-Type").getValueAsString());
        assertTrue(record.has("/Content"));
        assertEquals(requestEntry.getValue(), record.get("/Content").getValueAsString());
      } finally {
        runner.runDestroy();
      }

    }

  }

  @Test
  public void testStreamingHttpWithNewlineOnly() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "nlstream";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    runBatchAndAssertNames(DataFormat.JSON, conf);
  }

  @Test
  public void testStreamingHttpWithXml() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "xmlstream";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.XML;
    conf.dataFormatConfig.xmlRecordElement = "record";

    runBatchAndAssertNames(DataFormat.XML, conf);
  }

  @Test
  public void testStreamingHttpWithText() throws Exception {
    doTestStreamingHttpWithText("textstream", 1000, false);

  }

  @Test // Tests SDC-4337
  public void testSlowStreamingHttpWithText() throws Exception {
    doTestStreamingHttpWithText("slowstream", 1000, true);

  }

  private void doTestStreamingHttpWithText(String endpoint, int timeout, boolean delayStream) throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + endpoint;
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = timeout;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.TEXT;

    runBatchAndAssertNames(DataFormat.TEXT, conf, delayStream);
  }

  @Test
  public void testHttpWithLinearBackoff() throws Exception {
    for (final HttpClientMode mode : HttpClientMode.values()) {
      // this should work for all modes
      HttpClientConfigBean conf = new HttpClientConfigBean();
      conf.client.authType = AuthenticationType.NONE;
      conf.httpMode = mode;
      conf.resourceUrl = getBaseUri() + "stream/linear-backoff-ok";
      conf.client.readTimeoutMillis = 0;
      conf.basic.maxBatchSize = 3;
      conf.basic.maxWaitTime = 10000;
      conf.pollingInterval = 1000;
      conf.httpMethod = HttpMethod.GET;
      conf.dataFormat = DataFormat.JSON;
      conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

      final HttpStatusResponseActionConfigBean linearBackoff = new HttpStatusResponseActionConfigBean(
          STATUS_SLOW_DOWN,
          MAX_NUM_REQUEST_RETRIES,
          BASELINE_BACKOFF_MS,
          ResponseAction.RETRY_LINEAR_BACKOFF
      );

      final HttpStatusResponseActionConfigBean failAction = new HttpStatusResponseActionConfigBean(
          STATUS_TEST_FAIL,
          MAX_NUM_REQUEST_RETRIES,
          BASELINE_BACKOFF_MS,
          ResponseAction.STAGE_ERROR
      );

      conf.responseStatusActionConfigs = new LinkedList<>();
      conf.responseStatusActionConfigs.add(linearBackoff);
      conf.responseStatusActionConfigs.add(failAction);

      runBatchAndAssertNames(DataFormat.JSON, conf);
      resetServerStatus();
    }
  }

  @Test
  public void testHttpWithExponentialBackoff() throws Exception {
    for (final HttpClientMode mode : HttpClientMode.values()) {
      // this should work for all modes
      final HttpClientConfigBean conf = new HttpClientConfigBean();
      conf.client.authType = AuthenticationType.NONE;
      conf.httpMode = mode;
      conf.resourceUrl = getBaseUri() + "stream/exp-backoff-ok";
      conf.client.readTimeoutMillis = 0;
      conf.basic.maxBatchSize = 3;
      conf.basic.maxWaitTime = 10000;
      conf.pollingInterval = 1000;
      conf.httpMethod = HttpMethod.GET;
      conf.dataFormat = DataFormat.JSON;
      conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

      final HttpStatusResponseActionConfigBean expBackoff = new HttpStatusResponseActionConfigBean(
          STATUS_SLOW_DOWN,
          MAX_NUM_REQUEST_RETRIES,
          BASELINE_BACKOFF_MS,
          ResponseAction.RETRY_EXPONENTIAL_BACKOFF
      );

      final HttpStatusResponseActionConfigBean failAction = new HttpStatusResponseActionConfigBean(
          STATUS_TEST_FAIL,
          MAX_NUM_REQUEST_RETRIES,
          BASELINE_BACKOFF_MS,
          ResponseAction.STAGE_ERROR
      );

      conf.responseStatusActionConfigs = new LinkedList<>();
      conf.responseStatusActionConfigs.add(expBackoff);
      conf.responseStatusActionConfigs.add(failAction);

      runBatchAndAssertNames(DataFormat.JSON, conf);
      resetServerStatus();
    }
  }



  @Test
  public void testGetNamesWithSlowStream() throws Exception {

    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "stream/slow-stream";
    conf.client.readTimeoutMillis = (int)SLOW_STREAM_UNIT_TIME*3;
    conf.basic.maxBatchSize = 3;
    conf.basic.maxWaitTime = 10000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    final HttpStatusResponseActionConfigBean expBackoff = new HttpStatusResponseActionConfigBean(
        STATUS_SLOW_DOWN,
        MAX_NUM_REQUEST_RETRIES,
        BASELINE_BACKOFF_MS,
        ResponseAction.RETRY_EXPONENTIAL_BACKOFF
    );

    final HttpStatusResponseActionConfigBean failAction = new HttpStatusResponseActionConfigBean(
        STATUS_TEST_FAIL,
        MAX_NUM_REQUEST_RETRIES,
        BASELINE_BACKOFF_MS,
        ResponseAction.STAGE_ERROR
    );

    conf.responseStatusActionConfigs = new LinkedList<>();
    conf.responseStatusActionConfigs.add(expBackoff);
    conf.responseStatusActionConfigs.add(failAction);

    conf.responseTimeoutActionConfig = new HttpTimeoutResponseActionConfigBean(0, ResponseAction.RETRY_IMMEDIATELY);

    /*
          1. newline (empty batch)
          2. newline (empty batch)
          3. newline (empty batch)
          4. no response
          5. no response
          6. no response (clienet timeout; empty batch should be returned at this point)
          7. newline (empty batch)
          8. data (names batch)
     */

    runBatchesAndAssertNames(
      DataFormat.JSON,
      conf,
      new String[][] {
          new String[0],
          EXPECTED_NAMES
      },
      false
    );
  }

  private List<Record> getRecords(SourceRunner runner) throws StageException {
    StageRunner.Output output = runner.runProduce(null, 1000);
    Map<String, List<Record>> recordMap = output.getRecords();
    return recordMap.get("lane");
  }

  @Test
  public void testNoAuthorizeHttpOnSendToError() throws Exception {
    HttpClientSource origin = getUnauthorizedClientSource();
    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
      .addOutputLane("lane")
      .setOnRecordError(OnRecordError.TO_ERROR)
      .build();
    runner.runInit();

    try {
      runner.runProduce(null, 1000);
      List<String> errors = runner.getErrors();
      assertEquals(1, errors.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoAuthorizeHttpOnStopPipeline() throws Exception {
    HttpClientSource origin = getUnauthorizedClientSource();
    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
      .addOutputLane("lane")
      .setOnRecordError(OnRecordError.STOP_PIPELINE)
      .build();
    runner.runInit();

    boolean exceptionThrown = false;

    try {
      runner.runProduce(null, 1000);
      List<String> errors = runner.getErrors();
      assertEquals(1, errors.size());
    } catch (StageException ex){
      exceptionThrown = true;
      assertEquals(ex.getErrorCode(), Errors.HTTP_01);
    }
    finally {
      runner.runDestroy();
    }

    assertTrue(exceptionThrown);
  }

  @Test
  public void testNoAuthorizeHttpOnDiscard() throws Exception {
    HttpClientSource origin = getUnauthorizedClientSource();

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
      .addOutputLane("lane")
      .setOnRecordError(OnRecordError.DISCARD)
      .build();
    runner.runInit();

    try {
      runner.runProduce(null, 1000);
      List<String> errors = runner.getErrors();
      assertEquals(0, errors.size());
    } finally {
      runner.runDestroy();
    }
  }

  private String extractValueFromRecord(Record r, DataFormat f) {
    String v = null;
    if (f == DataFormat.JSON) {
      v = r.get("/name").getValueAsString();
    } else if (f == DataFormat.TEXT) {
      v = r.get().getValueAsMap().get("text").getValueAsString();
    } else if (f == DataFormat.XML) {
      v = r.get().getValueAsMap().get("name").getValueAsList().get(0).getValueAsMap().get("value").getValueAsString();
    }
    return v;
  }

  private HttpClientSource getUnauthorizedClientSource() {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "unauthorized";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;
    conf.client.useProxy = false;

    return new HttpClientSource(conf);
  }

  @Test
  public void testUniversalAuth() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.UNIVERSAL;
    conf.client.basicAuth.username = "foo";
    conf.client.basicAuth.password = "bar";
    conf.httpMode = HttpClientMode.POLLING;
    conf.resourceUrl = getBaseUri() + "auth";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 10000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      List<Record> parsedRecords = getRecords(runner);

      assertEquals(3, parsedRecords.size());

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertTrue(parsedRecords.get(i).has("/name"));
        assertEquals(EXPECTED_NAMES[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.JSON));
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoWWWAuthenticate() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.BASIC;
    conf.client.basicAuth.username = "foo";
    conf.client.basicAuth.password = "bar";
    conf.httpMode = HttpClientMode.POLLING;
    conf.resourceUrl = getBaseUri() + "preemptive";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 10000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    runBatchAndAssertNames(DataFormat.JSON, conf);
  }

  @Test
  public void testStreamingHttpWithHeader() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.headers.put("abcdef", "ghijkl");
    conf.resourceUrl = getBaseUri() + "headers";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      List<Record> parsedRecords = getRecords(runner);

      assertEquals(3, parsedRecords.size());

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertTrue(parsedRecords.get(i).has("/name"));
        // Grizzly is from some reason lower-casing the header attribute names. That is however correct as RFC 2616 clearly
        // states that header names are case-insensitive.
        assertEquals("StreamSets", parsedRecords.get(i).getHeader().getAttribute("x-test-header"));
        assertEquals("[a, b]", parsedRecords.get(i).getHeader().getAttribute("x-list-header"));
        assertEquals(EXPECTED_NAMES[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.JSON));
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidELs() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.POLLING;
    conf.headers.put("abcdef", "${invalid:el()}");
    conf.resourceUrl = getBaseUri() + "${invalid:el()}";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.POST;
    conf.requestBody = "${invalid:el()}";
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(3, issues.size());
  }

  @Test
  public void testValidELs() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.POLLING;
    conf.headers.put("abcdef", "${str:trim('abcdef ')}");
    conf.resourceUrl = getBaseUri() + "${str:trim('abcdef ')}";
    conf.client.readTimeoutMillis = 1000;
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.POST;
    conf.requestBody = "${str:trim('abcdef ')}";
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());
  }
}
