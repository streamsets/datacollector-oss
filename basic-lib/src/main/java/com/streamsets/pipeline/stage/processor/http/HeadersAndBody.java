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

package com.streamsets.pipeline.stage.processor.http;

import com.streamsets.pipeline.lib.http.HttpMethod;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedMap;

public class HeadersAndBody {
  final MultivaluedMap<String, Object> resolvedHeaders;
  final String requestBody;
  final String contentType;
  final HttpMethod method;
  final WebTarget target;

  HeadersAndBody(
      MultivaluedMap<String, Object> headers,
      String requestBody,
      String contentType,
      HttpMethod method,
      WebTarget target
  ) {
    this.resolvedHeaders = headers;
    this.requestBody = requestBody;
    this.contentType = contentType;
    this.method = method;
    this.target = target;
  }
}
