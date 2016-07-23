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
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Test;

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
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Currently tests do not include basic auth because of lack of support in JerseyTest
 * so we trust that the Jersey client we use implements auth correctly.
 */
public class HttpClientSourceIT extends JerseyTest {

  @Path("/stream")
  @Produces("application/json")
  public static class StreamResource {
    @GET
    public Response getStream() {
      return Response.ok(
          "{\"name\": \"adam\"}\r\n" +
          "{\"name\": \"joe\"}\r\n" +
          "{\"name\": \"sally\"}"
      ).build();
    }

    @POST
    public Response postStream(String name) {
      Map<String, String> map = ImmutableMap.of("adam", "adam", "joe", "joe", "sally", "sally");
      String queriedName = map.get(name);
      return Response.ok(
          "{\"name\": \"" + queriedName + "\"}\r\n"
      ).build();
    }
  }

  @Path("/nlstream")
  @Produces("application/json")
  public static class NewlineStreamResource {
    @GET
    public Response getStream() {
      return Response.ok(
          "{\"name\": \"adam\"}\n" +
          "{\"name\": \"joe\"}\n" +
          "{\"name\": \"sally\"}"
      ).build();
    }
  }

  @Path("/xmlstream")
  @Produces("application/xml")
  public static class XmlStreamResource {
    @GET
    public Response getStream() {
      return Response.ok(
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
      ).build();
    }
  }

  @Path("/textstream")
  @Produces("application/text")
  public static class TextStreamResource {
    @GET
    public Response getStream() {
      return Response.ok(
          "adam\r\n" +
          "joe\r\n" +
          "sally"
      ).build();
    }
  }

  @Path("/headers")
  public static class HeaderRequired {
    @GET
    public Response getWithHeader(@Context HttpHeaders h) {
      // This endpoint will fail if a magic header isnt included
      String headerValue = h.getRequestHeaders().getFirst("abcdef");
      assertNotNull(headerValue);
      return Response.ok(
          "{\"name\": \"adam\"}\r\n" +
              "{\"name\": \"joe\"}\r\n" +
              "{\"name\": \"sally\"}"
      ).build();
    }
  }

  @Path("/preemptive")
  public static class PreemptiveAuthResource {

    @GET
    public Response get(@Context HttpHeaders h) {
      // This endpoint will fail if universal is used and expects preemptive auth (basic)
      String value = h.getRequestHeaders().getFirst("Authorization");
      assertNotNull(value);
      return Response.ok(
          "{\"name\": \"adam\"}\r\n" +
              "{\"name\": \"joe\"}\r\n" +
              "{\"name\": \"sally\"}"
      ).build();
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

      return Response.ok(
          "{\"name\": \"adam\"}\r\n" +
              "{\"name\": \"joe\"}\r\n" +
              "{\"name\": \"sally\"}"
      ).build();
    }
  }

  @Path("/unauthorized")
  @Singleton
  public static class AlwaysUnauthorized {
    @GET
    public Response get() {
      return Response
          .status(401)
          .header("WWW-Authenticate", "Basic realm=\"WallyWorld\"")
          .build();
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
            XmlStreamResource.class,
            PreemptiveAuthResource.class,
            AuthResource.class,
            HeaderRequired.class,
            AlwaysUnauthorized.class
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
                    XmlStreamResource.class,
                    PreemptiveAuthResource.class,
                    AuthResource.class,
                    HeaderRequired.class,
                    AlwaysUnauthorized.class
                )
            )
        )
    ).build();
  }

  @Test
  public void testStreamingHttp() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "stream";
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(3, parsedRecords.size());

      String[] names = { "adam", "joe", "sally" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertTrue(parsedRecords.get(i).has("/name"));
        assertEquals(names[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.JSON));
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
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.POST;
    conf.requestBody = "adam";
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

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
  public void testStreamingHttpWithNewlineOnly() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "nlstream";
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(3, parsedRecords.size());

      String[] names = { "adam", "joe", "sally" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertTrue(parsedRecords.get(i).has("/name"));
        assertEquals(names[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.JSON));
      }
    } finally {
      runner.runDestroy();
    }

  }

  @Test
  public void testStreamingHttpWithXml() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "xmlstream";
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.XML;
    conf.dataFormatConfig.xmlRecordElement = "record";
    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(3, parsedRecords.size());

      String[] names = { "adam", "joe", "sally" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertTrue(parsedRecords.get(i).has("/name"));
        assertEquals(names[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.XML));
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStreamingHttpWithText() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.resourceUrl = getBaseUri() + "textstream";
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.TEXT;
    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(3, parsedRecords.size());

      String[] names = { "adam", "joe", "sally" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertEquals(names[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.TEXT));
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNoAuthorizeHttpOnSendToError() throws Exception {
    HttpClientSource origin = getUnauthorizedClientSource();
    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
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
    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
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

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
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
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
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
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 10000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(3, parsedRecords.size());

      String[] names = { "adam", "joe", "sally" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertTrue(parsedRecords.get(i).has("/name"));
        assertEquals(names[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.JSON));
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
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 10000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(3, parsedRecords.size());

      String[] names = { "adam", "joe", "sally" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertTrue(parsedRecords.get(i).has("/name"));
        assertEquals(names[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.JSON));
      }
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testStreamingHttpWithHeader() throws Exception {
    HttpClientConfigBean conf = new HttpClientConfigBean();
    conf.client.authType = AuthenticationType.NONE;
    conf.httpMode = HttpClientMode.STREAMING;
    conf.headers.put("abcdef", "ghijkl");
    conf.resourceUrl = getBaseUri() + "headers";
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.GET;
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(3, parsedRecords.size());

      String[] names = { "adam", "joe", "sally" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        assertTrue(parsedRecords.get(i).has("/name"));
        assertEquals(names[i], extractValueFromRecord(parsedRecords.get(i), DataFormat.JSON));
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
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.POST;
    conf.requestBody = "${invalid:el()}";
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
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
    conf.client.requestTimeoutMillis = 1000;
    conf.entityDelimiter = "\r\n";
    conf.basic.maxBatchSize = 100;
    conf.basic.maxWaitTime = 1000;
    conf.pollingInterval = 1000;
    conf.httpMethod = HttpMethod.POST;
    conf.requestBody = "${str:trim('abcdef ')}";
    conf.dataFormat = DataFormat.JSON;
    conf.dataFormatConfig.jsonContent = JsonMode.MULTIPLE_OBJECTS;

    HttpClientSource origin = new HttpClientSource(conf);

    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(3, issues.size());
  }
}
