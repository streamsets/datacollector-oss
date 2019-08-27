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
package com.streamsets.pipeline.lib.parser.avro;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParserFactoryBuilder;
import com.streamsets.pipeline.lib.parser.DataParserFormat;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.Header;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import static com.streamsets.pipeline.config.OriginAvroSchemaSource.REGISTRY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_REPO_URLS_KEY;
import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.SCHEMA_SOURCE_KEY;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.Header.header;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.HttpStatusCode.OK_200;

public class TestAvroDataParserFactory {
  private static final Header contentJson = header("Content-Type", "application/vnd.schemaregistry.v1+json");
  private static final String localhost = "127.0.0.1";

  private static final String AVRO_SCHEMA_1 = "{\n" +
      "    \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"myrecord\\\",\\\"fields\\\":[{\\\"name\\\":\\\"f1\\\",\\\"type\\\":\\\"string\\\"}]}\"\n" +
      "}";
  private static final String AVRO_SCHEMA_2 = "{\n" +
      "    \"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"myrecord\\\",\\\"fields\\\":[{\\\"name\\\":\\\"f1\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"f2\\\",\\\"type\\\":\\\"string\\\",\\\"default\\\":\\\"bob\\\"}]}\"\n" +
      "}";
  public static final String ID = "id";
  public static final String OFFSET = "0";

  @Rule
  public MockServerRule mockServerRule = new MockServerRule(this);

  private final String address = "http://" + localhost + ":" + mockServerRule.getPort();

  @Test
  public void testInlineSchemaFromRegistry() throws Exception {
    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
            request()
                .withMethod("GET")
                .withPath("/schemas/ids/61"),
            exactly(1)
        )
        .respond(
            response()
                .withStatusCode(OK_200.code())
                .withHeader(contentJson)
                .withBody(AVRO_SCHEMA_1)
        );

    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
            request()
                .withMethod("GET")
                .withPath("/schemas/ids/62"),
            exactly(1)
        )
        .respond(
            response()
                .withStatusCode(OK_200.code())
                .withHeader(contentJson)
                .withBody(AVRO_SCHEMA_2)
        );

    Stage.Context context = ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
    DataParserFactory dataParserFactory = new DataParserFactoryBuilder(context, DataParserFormat.AVRO)
        .setCharset(Charset.forName("UTF-16"))
        .setConfig(SCHEMA_SOURCE_KEY, REGISTRY)
        .setConfig(SCHEMA_REPO_URLS_KEY, new ArrayList<>(Arrays.asList(address)))
        .setMaxDataLen(1024 * 1024)
        .build();

    // Check that AvroDataParserFactory uses the registered schema
    try (InputStream is = Resources.getResource("message1.avro").openStream()) {
      byte[] data = ByteStreams.toByteArray(is);

      DataParser parser = dataParserFactory.getParser(ID, data);
      Record record = parser.parse();
      Assert.assertNotEquals(null, record);
      Assert.assertEquals(record.get("/f1").getValue(), "value1");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    // Check that AvroDataParserFactory switches to the new schema for the second message
    try (InputStream is = Resources.getResource("message2.avro").openStream()) {
      byte[] data = ByteStreams.toByteArray(is);

      // use ByteArrayInputStream to confirm workaround in SDC-12367
      DataParser parser = dataParserFactory.getParser(ID, new ByteArrayInputStream(data), "0");
      Record record = parser.parse();
      Assert.assertNotEquals(null, record);
      Assert.assertEquals(record.get("/f1").getValue(), "value1");
      Assert.assertEquals(record.get("/f2").getValue(), "value2");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}
