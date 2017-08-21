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
package com.streamsets.lib.security.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class TestRestClient {

  @Test
  public void testBuilderAndClientCreation() throws Exception {
    RestClient.Builder builder = RestClient.builder("http://foo/bar");

    Assert.assertEquals("RestClient", builder.name);
    builder.name("name");
    Assert.assertEquals("name", builder.name);

    Assert.assertEquals("http://foo/bar/", builder.baseUrl);

    Assert.assertNull(builder.path);
    builder.path("path");
    Assert.assertEquals("path", builder.path);

    Assert.assertTrue(builder.csrf);
    builder.csrf(false);
    Assert.assertFalse(builder.csrf);

    Assert.assertTrue(builder.json);
    builder.json(false);
    Assert.assertFalse(builder.json);

    Assert.assertNull(builder.componentId);
    builder.componentId("componentId");
    Assert.assertEquals("componentId", builder.componentId);

    Assert.assertNull(builder.appAuthToken);
    builder.appAuthToken("appAuthToken");
    Assert.assertEquals("appAuthToken", builder.appAuthToken);

    Assert.assertTrue(builder.headers.isEmpty());
    builder.header("a", "A");
    Assert.assertEquals(1, builder.headers.size());
    Assert.assertTrue(builder.headers.containsKey("a"));
    Assert.assertEquals(ImmutableList.of("A"), builder.headers.get("a"));

    Assert.assertTrue(builder.queryParams.isEmpty());
    builder.queryParam("b", "B");
    Assert.assertEquals(1, builder.queryParams.size());
    Assert.assertTrue(builder.queryParams.containsKey("b"));
    Assert.assertEquals(ImmutableList.of("B"), builder.queryParams.get("b"));

    Assert.assertEquals(30 * 1000, builder.timeoutMillis);
    builder.timeout(10);
    Assert.assertEquals(10, builder.timeoutMillis);

    Assert.assertSame(RestClient.OBJECT_MAPPER, builder.jsonMapper);
    ObjectMapper mapper = new ObjectMapper();
    builder.jsonMapper(mapper);
    Assert.assertSame(mapper, builder.jsonMapper);

    RestClient client = builder.build();

    Assert.assertNotNull(client);
    Assert.assertEquals("http://foo/bar/", client.baseUrl);
    Assert.assertEquals("path", client.path);

    Map expectedHeaders = ImmutableMap.of(
        "a", ImmutableList.of("A"),
        SSOConstants.X_APP_COMPONENT_ID.toLowerCase(), ImmutableList.of("componentId"),
        SSOConstants.X_APP_AUTH_TOKEN.toLowerCase(), ImmutableList.of("appAuthToken")
    );

    Assert.assertEquals(3, client.headers.size());
    Assert.assertTrue(client.headers.containsKey("a"));
    Assert.assertEquals(expectedHeaders, client.headers);
    Assert.assertNotSame(builder.headers, client.headers);

    Assert.assertFalse(client.json);

    Assert.assertEquals(1, client.queryParams.size());
    Assert.assertTrue(client.queryParams.containsKey("b"));
    Assert.assertEquals(ImmutableList.of("B"), client.queryParams.get("b"));
    Assert.assertNotSame(builder.queryParams, client.queryParams);

    Assert.assertEquals(10, client.timeoutMillis);
    Assert.assertSame(mapper, client.jsonMapper);

    Assert.assertNotNull(client.conn);
    Assert.assertEquals(new URL("http://foo/bar/path?b=B"), client.conn.getURL());
    Assert.assertEquals(expectedHeaders, client.conn.getRequestProperties());
    Assert.assertEquals(10, client.conn.getConnectTimeout());
    Assert.assertEquals(10, client.conn.getReadTimeout());
    Assert.assertFalse(client.conn.getDefaultUseCaches());

    builder.path(null).queryParam("b", null).json(true).csrf(true);

    expectedHeaders =
        ImmutableMap.builder()
            .putAll(expectedHeaders)
            .put(SSOConstants.X_REST_CALL.toLowerCase(), ImmutableList.of("name"))
            .put(RestClient.CONTENT_TYPE, ImmutableList.of(RestClient.APPLICATION_JSON))
            .put(RestClient.ACCEPT, ImmutableList.of(RestClient.APPLICATION_JSON))
            .build();

    client = builder.build();
    Assert.assertEquals(new URL("http://foo/bar/"), client.conn.getURL());
    Assert.assertEquals(expectedHeaders, client.conn.getRequestProperties());
  }

  @Test
  public void testClientGet() throws Exception {
    RestClient client = Mockito.spy(RestClient.builder("http://foo").build());

    RestClient.Response response = Mockito.mock(RestClient.Response.class);

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.doReturn(conn).when(client).getConnection();
    Mockito
        .doReturn(response)
        .when(client).doHttp(Mockito.eq(conn), Mockito.eq("GET"), Mockito.any());
    Assert.assertEquals(response, client.get());
    Mockito.verify(conn, Mockito.times(1)).setDoOutput(Mockito.eq(true));
  }

  @Test
  public void testClientPost() throws Exception {
    RestClient client = Mockito.spy(RestClient.builder("http://foo").build());

    RestClient.Response response = Mockito.mock(RestClient.Response.class);

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.doReturn(conn).when(client).getConnection();
    Object data = new Object();
    Mockito
        .doReturn(response)
        .when(client).doHttp(Mockito.eq(conn), Mockito.eq("POST"), Mockito.eq(data));
    Assert.assertEquals(response, client.post(data));
    Mockito.verify(conn, Mockito.times(1)).setDoOutput(Mockito.eq(true));
    Mockito.verify(conn, Mockito.times(1)).setDoInput(Mockito.eq(true));
  }

  @Test
  public void testClientPut() throws Exception {
    RestClient client = Mockito.spy(RestClient.builder("http://foo").build());

    RestClient.Response response = Mockito.mock(RestClient.Response.class);

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.doReturn(conn).when(client).getConnection();
    Object data = new Object();
    Mockito
        .doReturn(response)
        .when(client).doHttp(Mockito.eq(conn), Mockito.eq("PUT"), Mockito.eq(data));
    Assert.assertEquals(response, client.put(data));
    Mockito.verify(conn, Mockito.times(1)).setDoOutput(Mockito.eq(true));
    Mockito.verify(conn, Mockito.times(1)).setDoInput(Mockito.eq(true));
  }

  @Test
  public void testClientDelete() throws Exception {
    RestClient client = Mockito.spy(RestClient.builder("http://foo").build());

    RestClient.Response response = Mockito.mock(RestClient.Response.class);

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.doReturn(conn).when(client).getConnection();
    Mockito
        .doReturn(response)
        .when(client).doHttp(Mockito.eq(conn), Mockito.eq("DELETE"), Mockito.any());
    Assert.assertEquals(response, client.delete());
    Mockito.verify(conn, Mockito.times(1)).setDoOutput(Mockito.eq(true));
  }

  @Test
  public void testDoHttp() throws Exception {
    RestClient client = Mockito.spy(RestClient.builder("http://foo").build());

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);

    RestClient.Response response = client.doHttp(conn, "METHOD", null);
    Assert.assertEquals(client.jsonMapper, response.jsonMapper);
    Assert.assertEquals(conn, response.conn);
    Mockito.verify(conn, Mockito.times(1)).setRequestMethod(Mockito.eq("METHOD"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Mockito.when(conn.getOutputStream()).thenReturn(baos);
    client.doHttp(conn, "METHOD", new HashMap());
    Assert.assertEquals("{}", new String(baos.toByteArray()));
  }

  @Test(expected = IllegalStateException.class)
  public void testDoHttpNotJson() throws Exception {
    RestClient client = Mockito.spy(RestClient.builder("http://foo").json(false).build());

    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.doReturn(conn).when(client).getConnection();
    Object data = new Object();
    client.post(data);
  }

  @Test
  public void testResponse() throws Exception {
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(100);
    Mockito.when(conn.getContentType()).thenReturn("application/json");
    Mockito.when(conn.getHeaderField(Mockito.eq("a"))).thenReturn("A");
    Map<String, List<String>> headers = new HashMap<>();
    Mockito.when(conn.getHeaderFields()).thenReturn(headers);
    InputStream inputStream = Mockito.mock(InputStream.class);
    Mockito.when(conn.getInputStream()).thenReturn(inputStream);


    RestClient.Response response = new RestClient.Response(new ObjectMapper(), conn);
    Assert.assertEquals(conn, response.getConnection());
    Assert.assertEquals(100, response.getStatus());
    Assert.assertEquals("application/json", response.getContentType());
    Assert.assertTrue(response.isJson());
    Assert.assertEquals("A", response.getHeader("a"));
    Assert.assertEquals(headers, response.getHeaders());
    Assert.assertEquals(inputStream, response.getInputStream());

    inputStream = new ByteArrayInputStream("[\"a\"]".getBytes());
    Mockito.when(conn.getInputStream()).thenReturn(inputStream);
    response = new RestClient.Response(new ObjectMapper(), conn);
    TypeReference<List<String>> typeReference = new TypeReference<List<String>>() {
    };
    List<String> list = response.getData(typeReference);
    Assert.assertNotNull(list);
    Assert.assertEquals("a", list.get(0));

    inputStream = new ByteArrayInputStream("{\"message\" : \"hello\"}".getBytes());
    Mockito.when(conn.getErrorStream()).thenReturn(inputStream);
    response = new RestClient.Response(new ObjectMapper(), conn);
    Assert.assertEquals(ImmutableMap.of("message", "hello"), response.getError());
  }

  @Test(expected = IllegalStateException.class)
  public void testResponseNoJson() throws Exception {
    HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
    Mockito.when(conn.getResponseCode()).thenReturn(100);
    Mockito.when(conn.getContentType()).thenReturn("foo");


    RestClient.Response response = new RestClient.Response(new ObjectMapper(), conn);
    response.getData(Object.class);
  }

}
