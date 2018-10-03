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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A REST Client with minimal dependencies (JDK & JACKSON -for JSON- )
 */
public class RestClient {

  @VisibleForTesting
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  static final String CONTENT_TYPE = "content-type";
  static final String ACCEPT = "accept";
  static final String APPLICATION_JSON = "application/json";

  public static class Builder {

    @VisibleForTesting
    String name;
    final String baseUrl;
    String path;
    boolean csrf;
    boolean json;
    String componentId;
    String appAuthToken;
    Map<String, List<String>> headers;
    Map<String, List<String>> queryParams;
    int timeoutMillis;
    ObjectMapper jsonMapper;

    private Builder(String baseUrl) {
      name = "RestClient";
      this.baseUrl = (baseUrl.endsWith("/")) ? baseUrl : baseUrl + "/";
      path = null;
      csrf = true;
      json = true;
      headers = new HashMap<>();
      queryParams = new HashMap<>();
      timeoutMillis = 30 * 1000;
      jsonMapper = OBJECT_MAPPER;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder csrf(boolean csrf) {
      this.csrf = csrf;
      return this;
    }

    public Builder jsonMapper(ObjectMapper mapper) {
      this.jsonMapper = mapper;
      return this;
    }

    public Builder componentId(String componentId) {
      this.componentId = componentId;
      return this;
    }

    public Builder appAuthToken(String appAuthToken) {
      this.appAuthToken = appAuthToken;
      return this;
    }

    public Builder json(boolean json) {
      this.json = json;
      return this;
    }

    Builder map(Map<String, List<String>> map, String name, String value) {
      if (value == null) {
        map.remove(name);
      } else {
        List<String> values = map.get(name);
        if (values == null) {
          values = new ArrayList<>();
          map.put(name, values);
        }
        values.add(value);
      }
      return this;
    }

    public Builder header(String name, String value) {
      return map(headers, name.toLowerCase(), value);
    }

    public Builder queryParam(String name, String value) {
      return map(queryParams, name, value);
    }

    public Builder timeout(int timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    public RestClient build() throws IOException {
      return build(path, queryParams);
    }

    public RestClient build(String path) throws IOException {
      return build(path, queryParams);
    }

    public RestClient build(Map<String, List<String>> queryParams) throws IOException {
      return build(path, queryParams);
    }

    public RestClient build(String path, Map<String, List<String>> queryParams) throws IOException {
      return new RestClient(name,
          baseUrl,
          path,
          componentId,
          appAuthToken,
          csrf,
          json,
          headers,
          queryParams,
          timeoutMillis,
          jsonMapper
      );
    }

  }

  public static class Response {

    @VisibleForTesting
    final ObjectMapper jsonMapper;
    final HttpURLConnection conn;

    Response(ObjectMapper jsonMapper, HttpURLConnection conn) {
      this.jsonMapper = jsonMapper;
      this.conn = conn;
    }

    public HttpURLConnection getConnection() {
      return conn;
    }

    public int getStatus() throws IOException {
      return conn.getResponseCode();
    }

    public String getContentType() {
      return conn.getContentType();
    }

    public boolean haveData() {
      return conn.getContentType() != null;
    }

    public boolean successful() throws IOException {
      return getStatus() >= 200 && getStatus() < 300;
    }

    public boolean isJson() {
      String contentType = conn.getContentType();
      return contentType != null && contentType.toLowerCase().trim().startsWith(APPLICATION_JSON);
    }

    public String getHeader(String headerName) {
      return conn.getHeaderField(headerName);
    }

    public Map<String, List<String>> getHeaders() {
      return conn.getHeaderFields();
    }

    public InputStream getInputStream() throws IOException {
      return conn.getInputStream();
    }

    public <T> T getData(TypeReference<T> typeReference) throws IOException {
      try(InputStream inputStream = getInputStream()) {
        return jsonMapper.readValue(inputStream, typeReference);
      }
    }

    public <T> T getData(Class<T> klass) throws IOException {
      if (isJson()) {
        try(InputStream inputStream = getInputStream()) {
          return jsonMapper.readValue(inputStream, klass);
        }
      } else {
        throw new IllegalStateException("Response is not application/json, is " + conn.getContentType());
      }
    }

    @SuppressWarnings("unchecked")
    public Map getError() throws IOException {
      Map map;
      if (isJson()) {
        try(InputStream inputStream = conn.getErrorStream()) {
          map = jsonMapper.readValue(inputStream, Map.class);
        }
      } else {
        map = new HashMap();
        map.put("message", conn.getResponseMessage());
      }
      return map;
    }

  }

  public static Builder builder(String baseUrl) {
    return new Builder(baseUrl);
  }

  @VisibleForTesting
  final String baseUrl;
  final Map<String, List<String>> headers;
  final Map<String, List<String>> queryParams;
  final String path;
  final boolean json;
  final int timeoutMillis;
  final ObjectMapper jsonMapper;
  HttpURLConnection conn;

  Map<String, List<String>> deepCopy(Map<String, List<String>> map) {
    Map<String, List<String>> copy = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      copy.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    return copy;
  }

  private RestClient(
      String name,
      String baseUrl,
      String path,
      String componentId,
      String appAuthToken,
      boolean csrf,
      boolean json,
      Map<String, List<String>> headers,
      Map<String, List<String>> queryParams,
      int timeoutMillis,
      ObjectMapper jsonMapper
  ) throws IOException {
    this.baseUrl = baseUrl;
    this.path = path;
    this.headers = deepCopy(headers);
    if (csrf && !this.headers.containsKey(SSOConstants.X_REST_CALL.toLowerCase())) {
      this.headers.put(SSOConstants.X_REST_CALL.toLowerCase(), Collections.singletonList(name));
    }
    this.json = json;
    if (json && !this.headers.containsKey(CONTENT_TYPE)) {
      this.headers.put(CONTENT_TYPE.toLowerCase(), Collections.singletonList(APPLICATION_JSON));
      this.headers.put(ACCEPT, Collections.singletonList(APPLICATION_JSON));
    }
    if (componentId != null && appAuthToken != null) {
      this.headers.put(SSOConstants.X_APP_COMPONENT_ID.toLowerCase(), Collections.singletonList(componentId));
      this.headers.put(SSOConstants.X_APP_AUTH_TOKEN.toLowerCase(), Collections.singletonList(appAuthToken));
    }
    this.queryParams = deepCopy(queryParams);
    this.timeoutMillis = timeoutMillis;
    this.jsonMapper = jsonMapper;
    reset();
  }

  HttpURLConnection createConnection() throws IOException {
    URL url = new URL(baseUrl);
    if (path != null) {
      url = new URL(url, path);
    }
    if (!queryParams.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      String separator = "?";
      for (Map.Entry<String, List<String>> param : queryParams.entrySet()) {
        for (String value : param.getValue()) {
          sb
              .append(separator)
              .append(URLEncoder.encode(param.getKey(), "UTF-8"))
              .append("=")
              .append(URLEncoder.encode(value, "UTF-8"));
          separator = "&";
        }
      }
      url = new URL(sb.insert(0, url.toExternalForm()).toString());
    }
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    for (Map.Entry<String, List<String>> header : headers.entrySet()) {
      for (String value : header.getValue()) {
        conn.addRequestProperty(header.getKey(), value);
      }
    }
    conn.setConnectTimeout(timeoutMillis);
    conn.setReadTimeout(timeoutMillis);
    conn.setDefaultUseCaches(false);
    return conn;
  }

  public void reset() throws IOException {
    conn = createConnection();
  }

  public HttpURLConnection getConnection() {
    return conn;
  }

  Response doHttp(HttpURLConnection conn, String method, Object upload) throws IOException {
    conn.setRequestMethod(method);
    if (upload != null) {
      if (json) {
        try(OutputStream outputStream = conn.getOutputStream()) {
          jsonMapper.writeValue(outputStream, upload);
        }
      } else {
        throw new IllegalStateException("Content type is not JSON, cannot upload data bean");
      }
    }
    return new Response(jsonMapper, conn);
  }

  public Response get() throws IOException {
    getConnection().setDoOutput(true);
    return doHttp(getConnection(), "GET", null);
  }

  public <T> Response post(T data) throws IOException {
    getConnection().setDoOutput(true);
    getConnection().setDoInput(true);
    return doHttp(getConnection(), "POST", data);
  }

  public <T> Response put(T data) throws IOException {
    getConnection().setDoOutput(true);
    getConnection().setDoInput(true);
    return doHttp(getConnection(), "PUT", data);
  }

  public Response delete() throws IOException {
    getConnection().setDoOutput(true);
    return doHttp(getConnection(), "DELETE", null);
  }

}
