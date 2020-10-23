/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.lib.security.http.aster;

import java.util.Map;

public interface AsterRestClient {
  enum RequestType {
    GET,
    POST,
    PUT,
    DELETE
  }

  /**
   * Request class for interfacing with Aster Service
   * @param <T> The payload type of the request
   */
  class Request<T> {
    private String resourcePath;
    private RequestType requestType;
    private T payload;
    private Class<T> payloadClass;
    private int timeout = -1;

    public String getResourcePath() {
      return resourcePath;
    }

    public Request<T> setResourcePath(String resourcePath) {
      this.resourcePath = resourcePath;
      return this;
    }

    public RequestType getRequestType() {
      return requestType;
    }

    public Request<T> setRequestType(RequestType requestType) {
      this.requestType = requestType;
      return this;
    }

    public T getPayload() {
      return payload;
    }

    public Request<T> setPayload(T payload) {
      this.payload = payload;
      return this;
    }

    public Class<T> getPayloadClass() {
      return payloadClass;
    }

    public Request<T> setPayloadClass(Class<T> payloadClass) {
      this.payloadClass = payloadClass;
      return this;
    }

    public int getTimeout() {
      return timeout;
    }

    public Request<T> setTimeout(int timeout) {
      this.timeout = timeout;
      return this;
    }
  }

  /**
   * Response class for {@link Request}
   * @param <T> The payload type of the request
   */
  class Response<T> {
    private int statusCode;
    private T body;
    private Map<String, String> headers;
    private String statusMessage;

    public int getStatusCode() {
      return statusCode;
    }

    public Response<T> setStatusCode(int statusCode) {
      this.statusCode = statusCode;
      return this;
    }

    public T getBody() {
      return body;
    }

    public Response<T> setBody(T body) {
      this.body = body;
      return this;
    }

    public Map<String, String> getHeaders() {
      return headers;
    }

    public Response<T> setHeaders(Map<String, String> headers) {
      this.headers = headers;
      return this;
    }

    public String getStatusMessage() {
      return statusMessage;
    }

    public Response<T> setStatusMessage(String statusMessage) {
      this.statusMessage = statusMessage;
      return this;
    }
  }

  /**
   * Returns if the client has Aster tokens already or not.
   */
  boolean hasTokens();

  /**
   * Rest Call Executed via {@link AsterRestClient}
   * @param request Aster Rest Request
   * @param <I> Payload type of the request
   * @param <O> Response body type
   * @return SSO Response
   */
  <I, O> Response<O> doRestCall(Request<I> request);
}
