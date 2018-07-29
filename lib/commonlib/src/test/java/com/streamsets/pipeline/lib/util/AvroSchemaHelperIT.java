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
package com.streamsets.pipeline.lib.util;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.streamsets.pipeline.config.DestinationAvroSchemaSource;
import com.streamsets.pipeline.config.OriginAvroSchemaSource;
import com.streamsets.pipeline.lib.data.DataFactory;
import org.apache.avro.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.Header;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.streamsets.pipeline.lib.util.AvroSchemaHelper.ID_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.Header.header;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.HttpStatusCode.OK_200;

public class AvroSchemaHelperIT {
  private static final Header contentJson = header("Content-Type", "application/vnd.schemaregistry.v1+json");
  private static final String localhost = "127.0.0.1";

    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);

    private final String address = "http://" + localhost + ":" + mockServerRule.getPort();

    @Test
    public void hasRegistryClient() throws Exception {
      assertFalse(new AvroSchemaHelper(getSettings(null, DestinationAvroSchemaSource.INLINE, false)).hasRegistryClient());
      assertTrue(new AvroSchemaHelper(getSettings(address, DestinationAvroSchemaSource.REGISTRY, false)).hasRegistryClient());
      assertTrue(new AvroSchemaHelper(getSettings(address, DestinationAvroSchemaSource.INLINE, true)).hasRegistryClient());
    }

    @Test
    public void loadFromRegistry() throws Exception {
      new MockServerClient(localhost, mockServerRule.getPort())
          .when(
              request()
                  .withMethod("GET")
                  .withPath("/schemas/ids/1"),
              exactly(1)
          )
          .respond(
              response()
                  .withStatusCode(OK_200.code())
                  .withHeader(contentJson)
                  .withBody(getBody("schema-registry/schema_1_resp.json"))
        );

      AvroSchemaHelper helper = new AvroSchemaHelper(getSettings(address, OriginAvroSchemaSource.REGISTRY, false));

      Schema schema = new Schema.Parser()
          .setValidate(true)
          .setValidateDefaults(true)
          .parse(getBody("schema-registry/schema_1.json"));

      assertEquals(schema, helper.loadFromRegistry(1));

      Map<String, Object> expectedDefaultValues = new HashMap<>();
      expectedDefaultValues.put("TestRecord.a", "");
      expectedDefaultValues.put("TestRecord.b", 0L);
      expectedDefaultValues.put("TestRecord.c", false);

      assertEquals(expectedDefaultValues, AvroSchemaHelper.getDefaultValues(schema));
  }

  @Test
  public void loadFromString() throws Exception {
    AvroSchemaHelper helper = new AvroSchemaHelper(getSettings(null, OriginAvroSchemaSource.INLINE, false));

    final String schemaString = getBody("schema-registry/schema_1.json");

    Schema schema = new Schema.Parser()
        .setValidate(true)
        .setValidateDefaults(true)
        .parse(schemaString);

    assertEquals(schema, helper.loadFromString(schemaString));
  }

  @Test
  public void registerSchema() throws Exception {
    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
            request()
                .withHeader(header("Content-Type", "application/vnd.schemaregistry.v1+json"))
                .withMethod("POST")
                .withPath("/subjects/topic1-value/versions"),
            exactly(1)
        )
        .respond(
            response()
                .withStatusCode(OK_200.code())
                .withHeader(contentJson)
                .withBody("{\"id\":1}")
        );

    AvroSchemaHelper helper = new AvroSchemaHelper(getSettings(address, DestinationAvroSchemaSource.INLINE, true));

    final String schemaString = getBody("schema-registry/schema_1.json");

    Schema schema = new Schema.Parser()
        .setValidate(true)
        .setValidateDefaults(true)
        .parse(schemaString);

    assertEquals(1, helper.registerSchema(schema, "topic1-value"));
  }

  @Test
  public void loadFromRegistryBySchemaId() throws Exception {
    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
            request()
                .withHeader(header("Content-Type", "application/vnd.schemaregistry.v1+json"))
                .withMethod("GET")
                .withPath("/schemas/ids/1"),
            exactly(1)
        )
        .respond(
            response()
                .withStatusCode(OK_200.code())
                .withHeader(contentJson)
                .withBody(getBody("schema-registry/schema_1_resp.json"))
        );

    AvroSchemaHelper helper = new AvroSchemaHelper(getSettings(address, OriginAvroSchemaSource.REGISTRY, false));

    final String schemaString = getBody("schema-registry/schema_1.json");

    Schema schema = new Schema.Parser()
        .setValidate(true)
        .setValidateDefaults(true)
        .parse(schemaString);

    assertEquals(schema, helper.loadFromRegistry(null, 1));
  }

  @Test
  public void loadFromRegistryBySubject() throws Exception {
    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
            request()
                .withHeader(header("Content-Type", "application/vnd.schemaregistry.v1+json"))
                .withMethod("GET")
                .withPath("/schemas/ids/1"),
            exactly(1)
        )
        .respond(
            response()
                .withStatusCode(OK_200.code())
                .withHeader(contentJson)
                .withBody(getBody("schema-registry/schema_1_resp.json"))
        );

    new MockServerClient(localhost, mockServerRule.getPort())
        .when(
            request()
                .withHeader(header("Content-Type", "application/vnd.schemaregistry.v1+json"))
                .withMethod("GET")
                .withPath("/subjects/topic1-value/versions/latest"),
            exactly(1)
        )
        .respond(
            response()
                .withStatusCode(OK_200.code())
                .withHeader(contentJson)
                .withBody(getBody("schema-registry/schema_1_subject_resp.json"))
        );

    AvroSchemaHelper helper = new AvroSchemaHelper(getSettings(address, OriginAvroSchemaSource.REGISTRY, false));

    final String schemaString = getBody("schema-registry/schema_1.json");

    Schema schema = new Schema.Parser()
        .setValidate(true)
        .setValidateDefaults(true)
        .parse(schemaString);

    assertEquals(schema, helper.loadFromRegistry("topic1-value", 0));
  }

  @Test
  public void writeSchemaId() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    AvroSchemaHelper helper = new AvroSchemaHelper(getSettings(null, DestinationAvroSchemaSource.INLINE, false));
    helper.writeSchemaId(out, 5000);

    ByteBuffer buf = ByteBuffer.wrap(out.toByteArray());
    assertEquals(AvroSchemaHelper.MAGIC_BYTE, buf.get());
    assertEquals(5000, buf.getInt());
  }

  @Test
  public void detectSchemaId() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(AvroSchemaHelper.MAGIC_BYTE);
    out.write(ByteBuffer.allocate(ID_SIZE).putInt(12345).array());

    AvroSchemaHelper helper = new AvroSchemaHelper(getSettings(null, OriginAvroSchemaSource.SOURCE, false));
    java.util.Optional<Integer> schemaId = helper.detectSchemaId(out.toByteArray());
    assertTrue(schemaId.isPresent());
    assertEquals(12345, (int) schemaId.get());
  }

  @Test
  public void detectSchemaIdNoMagicByte() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(0x01);
    out.write(ByteBuffer.allocate(ID_SIZE).putInt(12345).array());

    AvroSchemaHelper helper = new AvroSchemaHelper(getSettings(null, OriginAvroSchemaSource.SOURCE, false));
    java.util.Optional<Integer> schemaId = helper.detectSchemaId(out.toByteArray());
    assertFalse(schemaId.isPresent());
  }

  @Test
  public void detectInvalidSchemaId() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(AvroSchemaHelper.MAGIC_BYTE);

    AvroSchemaHelper helper = new AvroSchemaHelper(getSettings(null, OriginAvroSchemaSource.SOURCE, false));
    java.util.Optional<Integer> schemaId = helper.detectSchemaId(out.toByteArray());
    assertEquals(Optional.empty(), schemaId);
  }

  private String getBody(String path) throws IOException {
    return Resources.toString(Resources.getResource(path), Charsets.UTF_8);
  }

  private DataFactory.Settings getSettings(String repoUrl, Object schemaSource, boolean registerSchema) {
    List<String> urls = new ArrayList<>();
    if (repoUrl != null) {
      urls.add(repoUrl);
    }
    DataFactory.Settings settings = mock(DataFactory.Settings.class);
    when(settings.getConfig(AvroSchemaHelper.SCHEMA_REPO_URLS_KEY)).thenReturn(urls);
    when(settings.getConfig(AvroSchemaHelper.SCHEMA_SOURCE_KEY)).thenReturn(schemaSource);
    when(settings.getConfig(AvroSchemaHelper.REGISTER_SCHEMA_KEY)).thenReturn(registerSchema);

    return settings;
  }
}
